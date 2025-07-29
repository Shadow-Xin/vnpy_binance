import urllib
import hashlib
import hmac
import time
from copy import copy
from datetime import datetime, timedelta
from enum import Enum
from time import sleep

from numpy import format_float_positional

from vnpy_evo.event import Event, EventEngine
from vnpy_evo.trader.constant import (
    Direction,
    Exchange,
    Product,
    Status,
    OrderType,
    Interval
)
from vnpy_evo.trader.gateway import BaseGateway
from vnpy_evo.trader.object import (
    TickData,
    OrderData,
    TradeData,
    AccountData,
    ContractData,
    PositionData,
    BarData,
    OrderRequest,
    CancelRequest,
    SubscribeRequest,
    HistoryRequest
)
from vnpy_evo.trader.event import EVENT_TIMER
from vnpy_evo.trader.utility import round_to, ZoneInfo
from vnpy_evo.rest import Request, RestClient, Response
from vnpy_evo.websocket import WebsocketClient
from .binance_inverse_gateway import BinanceInverseDataWebsocketApi
from .binance_linear_gateway import BinanceLinearDataWebsocketApi

# Timezone constant
UTC_TZ = ZoneInfo("UTC")

# Real server hosts
P_REST_HOST: str = "https://papi.binance.com"
P_WEBSOCKET_TRADE_HOST: str = "wss://fstream.binance.com/pm/ws/"
# P_WEBSOCKET_DATA_HOST: str = "wss://fstream.binance.com/pm/stream"

# Testnet server hosts
P_TESTNET_REST_HOST: str = "https://testnet.binancefuture.com"
P_TESTNET_WEBSOCKET_TRADE_HOST: str = "wss://testnet.binancefuture.com/ws/"
P_TESTNET_WEBSOCKET_DATA_HOST: str = "wss://testnet.binancefuture.com/stream"

F_REST_HOST: str = "https://fapi.binance.com"
F_TESTNET_REST_HOST: str = "https://testnet.binancefuture.com"

D_REST_HOST: str = "https://dapi.binance.com"
D_TESTNET_REST_HOST: str = "https://testnet.binancefuture.com"

# Order status map
STATUS_BINANCES2VT: dict[str, Status] = {
    "NEW": Status.NOTTRADED,
    "PARTIALLY_FILLED": Status.PARTTRADED,
    "FILLED": Status.ALLTRADED,
    "CANCELED": Status.CANCELLED,
    "REJECTED": Status.REJECTED,
    "EXPIRED": Status.CANCELLED
}

# Order type map
ORDERTYPE_VT2BINANCES: dict[OrderType, tuple[str, str]] = {
    OrderType.LIMIT: ("LIMIT", "GTC"),
    OrderType.MARKET: ("MARKET", "GTC"),
    OrderType.FAK: ("LIMIT", "IOC"),
    OrderType.FOK: ("LIMIT", "FOK"),
}
ORDERTYPE_BINANCES2VT: dict[tuple[str, str], OrderType] = {v: k for k, v in ORDERTYPE_VT2BINANCES.items()}

# Direction map
DIRECTION_VT2BINANCES: dict[Direction, str] = {
    Direction.LONG: "BUY",
    Direction.SHORT: "SELL"
}
DIRECTION_BINANCES2VT: dict[str, Direction] = {v: k for k, v in DIRECTION_VT2BINANCES.items()}

# Kline interval map
INTERVAL_VT2BINANCES: dict[Interval, str] = {
    Interval.MINUTE: "1m",
    Interval.HOUR: "1h",
    Interval.DAILY: "1d",
}

# Timedelta map
TIMEDELTA_MAP: dict[Interval, timedelta] = {
    Interval.MINUTE: timedelta(minutes=1),
    Interval.HOUR: timedelta(hours=1),
    Interval.DAILY: timedelta(days=1),
}

# Set weboscket timeout to 24 hour
WEBSOCKET_TIMEOUT = 24 * 60 * 60


# Authentication level
class Security(Enum):
    NONE: int = 0
    SIGNED: int = 1
    API_KEY: int = 2


class BinancePortfolioMarginGateway(BaseGateway):
    """
    The Binance Portfolio Margin trading gateway for VeighNa.
    """

    default_name: str = "BINANCE_PORTFOLIO_MARGIN"

    default_setting: dict = {
        "API Key": "",
        "API Secret": "",
        "Server": ["REAL", "TESTNET"],
        "Kline Stream": ["False", "True"],
        "Proxy Host": "",
        "Proxy Port": 0
    }

    exchanges: Exchange = [Exchange.BINANCE]

    def __init__(self, event_engine: EventEngine, gateway_name: str) -> None:
        """
        The init method of the gateway.

        event_engine: the global event engine object of VeighNa
        gateway_name: the unique name for identifying the gateway
        """
        super().__init__(event_engine, gateway_name)

        self.trade_ws_api: PortfolioMarginTradeWebsocketApi = PortfolioMarginTradeWebsocketApi(self)
        self.rest_api: PortfolioMarginRestApi = PortfolioMarginRestApi(self)

        self.um_rest_api: BinanceLinearRestApi = BinanceLinearRestApi(self)
        self.cm_rest_api:BinanceInverseRestApi = BinanceInverseRestApi(self)

        self.cm_market_ws_api: BinanceInverseDataWebsocketApi = BinanceInverseDataWebsocketApi(self)
        self.um_market_ws_api: BinanceLinearDataWebsocketApi = BinanceLinearDataWebsocketApi(self)

        self.orders: dict[str, OrderData] = {}

    def connect(self, setting: dict) -> None:
        """Start server connections"""
        key: str = setting["API Key"]
        secret: str = setting["API Secret"]
        server: str = setting["Server"]
        proxy_host: str = setting["Proxy Host"]
        proxy_port: int = setting["Proxy Port"]

        kline_stream: bool = setting["Kline Stream"] == "True"

        self.rest_api.connect(key, secret, server, proxy_host, proxy_port)

        self.um_rest_api.connect(key, secret, server, proxy_host, proxy_port)
        self.cm_rest_api.connect(key, secret, server, proxy_host, proxy_port)

        self.cm_market_ws_api.connect(server, kline_stream, proxy_host, proxy_port)
        self.um_market_ws_api.connect(server, kline_stream, proxy_host, proxy_port)

        self.event_engine.register(EVENT_TIMER, self.process_timer_event)

    def subscribe(self, req: SubscribeRequest) -> None:
        """Subscribe market data"""
        if 'USDT' in req.symbol:
            self.um_market_ws_api.subscribe(req)
        else:
            self.cm_market_ws_api.subscribe(req)

    def send_order(self, req: OrderRequest) -> str:
        """Send new order"""
        return self.rest_api.send_order(req)

    def cancel_order(self, req: CancelRequest) -> None:
        """Cancel existing order"""
        self.rest_api.cancel_order(req)

    def query_account(self) -> None:
        """Not required since Binance provides websocket update"""
        pass

    def query_position(self) -> None:
        """Not required since Binance provides websocket update"""
        pass

    def query_history(self, req: HistoryRequest) -> list[BarData]:
        """Query kline history data"""
        if 'USDT' in req.symbol:
            return self.um_rest_api.query_history(req)
        else:
            return self.cm_rest_api.query_history(req)

    def close(self) -> None:
        """Close server connections"""
        self.rest_api.stop()
        self.um_rest_api.stop()
        self.cm_rest_api.stop()
        self.trade_ws_api.stop()
        self.cm_market_ws_api.stop()
        self.um_market_ws_api.stop()

    def process_timer_event(self, event: Event) -> None:
        """Process timer task"""
        self.rest_api.keep_user_stream()
        if datetime.now().second % 10 == 0:
            self.rest_api.query_position()
            self.rest_api.query_account()
        
    def on_order(self, order: OrderData) -> None:
        """"Save a copy of order and then push"""
        self.orders[order.orderid] = copy(order)
        super().on_order(order)

    def get_order(self, orderid: str) -> OrderData:
        """Get previously saved order"""
        return self.orders.get(orderid, None)


class PortfolioMarginRestApi(RestClient):
    """The REST API of PortfolioMarginGateway"""

    def __init__(self, gateway: BinancePortfolioMarginGateway) -> None:
        """
        The init method of the api.

        gateway: the parent gateway object for pushing callback data.
        """
        super().__init__()

        self.gateway: BinancePortfolioMarginGateway = gateway
        self.gateway_name: str = gateway.gateway_name
        self.trade_ws_api: PortfolioMarginTradeWebsocketApi = self.gateway.trade_ws_api

        self.key: str = ""
        self.secret: str = ""

        self.user_stream_key: str = ""
        self.keep_alive_count: int = 0
        self.time_offset: int = 0

        self.order_count: int = 1_000_000
        self.order_prefix: str = ""

    def sign(self, request: Request) -> Request:
        """Standard callback for signing a request"""
        security: Security = request.data["security"]
        if security == Security.NONE:
            request.data = None
            return request

        if request.params:
            path: str = request.path + "?" + urllib.parse.urlencode(request.params)
        else:
            request.params = dict()
            path: str = request.path

        if security == Security.SIGNED:
            timestamp: int = int(time.time() * 1000)

            if self.time_offset > 0:
                timestamp -= abs(self.time_offset)
            elif self.time_offset < 0:
                timestamp += abs(self.time_offset)

            request.params["timestamp"] = timestamp

            query: str = urllib.parse.urlencode(sorted(request.params.items()))
            signature: bytes = hmac.new(
                self.secret,
                query.encode("utf-8"),
                hashlib.sha256
            ).hexdigest()

            query += "&signature={}".format(signature)
            path: str = request.path + "?" + query

        request.path = path
        request.params = {}
        request.data = {}

        headers = {
            "Content-Type": "application/x-www-form-urlencoded",
            "Accept": "application/json",
            "X-MBX-APIKEY": self.key,
            "Connection": "close"
        }

        if security in [Security.SIGNED, Security.API_KEY]:
            request.headers = headers

        return request

    def connect(
        self,
        key: str,
        secret: str,
        server: str,
        proxy_host: str,
        proxy_port: int
    ) -> None:
        """Start server connection"""
        self.key = key
        self.secret = secret.encode()
        self.proxy_port = proxy_port
        self.proxy_host = proxy_host
        self.server = server

        self.order_prefix = datetime.now().strftime("%y%m%d%H%M%S")

        if self.server == "REAL":
            self.init(P_REST_HOST, proxy_host, proxy_port)
        else:
            self.init(P_TESTNET_REST_HOST, proxy_host, proxy_port)

        self.start()

        self.gateway.write_log("Portfolio Margin REST API started")

        self.query_time()
        self.query_order()
        self.query_account()
        self.query_position()
        self.start_user_stream()

    def query_time(self) -> None:
        """Query server time"""
        data: dict = {"security": Security.NONE}
        path: str = "/papi/v1/time"

        return self.add_request(
            "GET",
            path,
            callback=self.on_query_time,
            data=data
        )

    def on_query_time(self, data: dict, request: Request) -> None:
        """Callback of server time query"""
        local_time: int = int(time.time() * 1000)
        server_time: int = int(data["serverTime"])
        self.time_offset: int = local_time - server_time

        self.gateway.write_log(f"Portfolio Margin Server time updated, local offset: {self.time_offset}ms")

    def query_account(self) -> None:
        """Query account balance"""

        data: dict = {"security": Security.SIGNED}
        path: str = "/papi/v1/balance"
        self.add_request(
            method="GET",
            path=path,
            callback=self.on_query_account,
            data=data
        )

    def on_query_account(self, data: list, request: Request) -> None:
        """Callback of account query"""
        for asset_data in data:
            account: AccountData = AccountData(
                accountid=asset_data["asset"],
                balance=float(asset_data["totalWalletBalance"]),
                frozen=float(asset_data["totalWalletBalance"])-float(asset_data["crossMarginFree"]),
                gateway_name=self.gateway_name
            )
            self.gateway.on_account(account)

        # self.gateway.write_log("Portfolio Margin Account data received")

    def query_position(self) -> None:
        """Query holding positions"""

        data: dict = {"security": Security.SIGNED}
        path: str = "/papi/v1/um/positionRisk"
        self.add_request(
            method="GET",
            path=path,
            callback=self.on_query_position,
            data=data
        )

        path: str = "/papi/v1/cm/positionRisk"
        self.add_request(
            method="GET",
            path=path,
            callback=self.on_query_position,
            data=data
        )

    def on_query_position(self, data: list, request: Request) -> None:
        """Callback of position query"""

        for d in data:
            position: PositionData = PositionData(
                symbol=d["symbol"],
                exchange=Exchange.BINANCE,
                direction=Direction.NET,
                volume=abs(float(d['positionAmt'])),
                price=float(d["entryPrice"]),
                pnl=float(d["unRealizedProfit"]),
                gateway_name=self.gateway_name,
            )
            self.gateway.on_position(position)
        
        # self.gateway.write_log("Portfolio Margin Position data received")

    def query_order(self) -> None:
        """Query open orders"""
        data: dict = {"security": Security.SIGNED}
        path: str = "/papi/v1/um/allOrders"
        self.add_request(
            method="GET",
            path=path,
            callback=self.on_query_order,
            data=data
        )

        data: dict = {"security": Security.SIGNED}
        path: str = "/papi/v1/cm/allOrders"
        self.add_request(
            method="GET",
            path=path,
            callback=self.on_query_order,
            data=data
        )

    def on_query_order(self, data: list, request: Request) -> None:
        """Callback of order query"""
        if not self.gateway.orders:
            for d in data:
                key: tuple[str, str] = (d["type"], d["timeInForce"])
                order_type: OrderType = ORDERTYPE_BINANCES2VT.get(key, None)
                if not order_type:
                    continue

                order: OrderData = OrderData(
                    symbol=d["symbol"],
                    exchange=Exchange.BINANCE,
                    orderid=d["clientOrderId"],
                    type=order_type,
                    direction=DIRECTION_BINANCES2VT[d["side"]],
                    price=float(d["price"]),
                    volume=float(d["origQty"]),
                    traded=float(d["executedQty"]),
                    status=STATUS_BINANCES2VT[d["status"]],
                    datetime=generate_datetime(d["time"]),
                    gateway_name=self.gateway_name,
                )
                self.gateway.on_order(order)

        self.gateway.write_log("Portfolio Margin Order data received")

    def send_order(self, req: OrderRequest) -> str:
        """Send new order"""
        # Generate new order id
        self.order_count += 1
        orderid: str = self.order_prefix + str(self.order_count)

        # Push a submitting order event
        order: OrderData = req.create_order_data(
            orderid,
            self.gateway_name
        )
        self.gateway.on_order(order)

        # Create order parameters
        data: dict = {"security": Security.SIGNED}

        params: dict = {
            "symbol": req.symbol,
            "side": DIRECTION_VT2BINANCES[req.direction],
            "quantity": format_float(req.volume),
            "newClientOrderId": orderid,
        }

        if req.type == OrderType.MARKET:
            params["type"] = "MARKET"
        else:
            order_type, time_condition = ORDERTYPE_VT2BINANCES[req.type]
            params["type"] = order_type
            params["timeInForce"] = time_condition
            params["price"] = format_float(req.price)

        if 'USDT' in req.symbol:
            path: str = "/papi/v1/um/order"
        else:
            path: str = "/papi/v1/cm/order"

        self.add_request(
            method="POST",
            path=path,
            callback=self.on_send_order,
            data=data,
            params=params,
            extra=order,
            on_error=self.on_send_order_error,
            on_failed=self.on_send_order_failed
        )

        return order.vt_orderid

    def on_send_order(self, data: dict, request: Request) -> None:
        """Callback of send order"""
        pass

    def on_send_order_failed(self, status_code: str, request: Request) -> None:
        """Callback of send order failed"""
        order: OrderData = request.extra
        order.status = Status.REJECTED
        self.gateway.on_order(order)

        msg: str = f"Order failed, status code: {status_code}, message: {request.response.text}"
        self.gateway.write_log(msg)

    def on_send_order_error(self, exception_type: type, exception_value: Exception, tb, request: Request) -> None:
        """Callback of send order error"""
        order: OrderData = request.extra
        order.status = Status.REJECTED
        self.gateway.on_order(order)

        msg: str = f"Order failed, traceback: {exception_value}"
        self.gateway.write_log(msg)

    def cancel_order(self, req: CancelRequest) -> None:
        """Cancel existing order"""
        data: dict = {"security": Security.SIGNED}

        params: dict = {
            "symbol": req.symbol,
            "origClientOrderId": req.orderid
        }
        if 'USDT' in req.symbol:
            path: str = "/papi/v1/um/order"
        else:
            path: str = "/papi/v1/cm/order"

        order: OrderData = self.gateway.get_order(req.orderid)

        self.add_request(
            method="DELETE",
            path=path,
            callback=self.on_cancel_order,
            params=params,
            data=data,
            on_failed=self.on_cancel_failed,
            extra=order
        )

    def on_cancel_order(self, data: dict, request: Request) -> None:
        """Callback of cancel order"""
        pass

    def on_cancel_failed(self, status_code: str, request: Request) -> None:
        """Callback of cancel order failed"""
        if request.extra:
            order: OrderData = request.extra
            self.gateway.write_log(f"Cancel order failed, orderid: {order.orderid}")

    def start_user_stream(self) -> Request:
        """Create listen key for user stream"""
        data: dict = {"security": Security.API_KEY}
        path: str = "/papi/v1/listenKey"
        self.add_request(
            method="POST",
            path=path,
            callback=self.on_start_user_stream,
            data=data
        )

    def on_start_user_stream(self, data: dict, request: Request) -> None:
        """Successful callback of start_user_stream"""
        self.user_stream_key = data["listenKey"]
        self.keep_alive_count = 0

        if self.server == "REAL":
            url = P_WEBSOCKET_TRADE_HOST + self.user_stream_key
        else:
            url = P_TESTNET_WEBSOCKET_TRADE_HOST + self.user_stream_key

        self.trade_ws_api.connect(url, self.proxy_host, self.proxy_port)

    def keep_user_stream(self) -> Request:
        """Extend listen key validity"""
        if not self.user_stream_key:
            return

        self.keep_alive_count += 1
        if self.keep_alive_count < 600:
            return
        self.keep_alive_count = 0

        data: dict = {"security": Security.API_KEY}
        params: dict = {"listenKey": self.user_stream_key}
        path: str = "/papi/v1/listenKey"

        self.add_request(
            method="PUT",
            path=path,
            callback=self.on_keep_user_stream,
            params=params,
            data=data,
            on_error=self.on_keep_user_stream_error
        )

    def on_keep_user_stream(self, data: dict, request: Request) -> None:
        """Successful callback of keep_user_stream"""
        pass

    def on_keep_user_stream_error(self, exception_type: type, exception_value: Exception, tb, request: Request) -> None:
        """Error callback of keep_user_stream"""
        if not issubclass(exception_type, TimeoutError):        # Ignore timeout exception
            self.on_error(exception_type, exception_value, tb, request)


class BinanceLinearRestApi(RestClient):
    """The REST API of BinanceLinearGateway"""

    def __init__(self, gateway: PortfolioMarginRestApi) -> None:
        """
        The init method of the api.

        gateway: the parent gateway object for pushing callback data.
        """
        super().__init__()

        self.gateway: PortfolioMarginRestApi = gateway
        self.gateway_name: str = gateway.gateway_name

        self.key: str = ""
        self.secret: bytes = b""

        self.user_stream_key: str = ""
        self.keep_alive_count: int = 0
        self.time_offset: int = 0

        self.order_count: int = 1_000_000
        self.order_prefix: str = ""

    def sign(self, request: Request) -> Request:
        """Standard callback for signing a request"""
        security: Security = request.data["security"]
        if security == Security.NONE:
            request.data = None
            return request

        if request.params:
            path: str = request.path + "?" + urllib.parse.urlencode(request.params)
        else:
            request.params = dict()
            path: str = request.path

        if security == Security.SIGNED:
            timestamp: int = int(time.time() * 1000)

            if self.time_offset > 0:
                timestamp -= abs(self.time_offset)
            elif self.time_offset < 0:
                timestamp += abs(self.time_offset)

            request.params["timestamp"] = timestamp

            query: str = urllib.parse.urlencode(sorted(request.params.items()))
            signature: bytes = hmac.new(
                self.secret,
                query.encode("utf-8"),
                hashlib.sha256
            ).hexdigest()

            query += "&signature={}".format(signature)
            path: str = request.path + "?" + query

        request.path = path
        request.params = {}
        request.data = {}

        # Add header to the request
        headers = {
            "Content-Type": "application/x-www-form-urlencoded",
            "Accept": "application/json",
            "X-MBX-APIKEY": self.key,
            "Connection": "close"
        }

        if security in [Security.SIGNED, Security.API_KEY]:
            request.headers = headers

        return request

    def connect(
        self,
        key: str,
        secret: str,
        server: str,
        proxy_host: str,
        proxy_port: int
    ) -> None:
        """Start server connection"""
        self.key = key
        self.secret = secret.encode()
        self.proxy_port = proxy_port
        self.proxy_host = proxy_host
        self.server = server

        self.order_prefix = datetime.now().strftime("%y%m%d%H%M%S")

        if self.server == "REAL":
            self.init(F_REST_HOST, proxy_host, proxy_port)
        else:
            self.init(F_TESTNET_REST_HOST, proxy_host, proxy_port)

        self.start()

        self.gateway.write_log("Linear REST API started")

        self.query_time()
        self.query_contract()

    def query_time(self) -> None:
        """Query server time"""
        data: dict = {"security": Security.NONE}

        path: str = "/fapi/v1/time"

        self.add_request(
            "GET",
            path,
            callback=self.on_query_time,
            data=data
        )

    def on_query_time(self, data: dict, request: Request) -> None:
        """Callback of server time query"""
        local_time: int = int(time.time() * 1000)
        server_time: int = int(data["serverTime"])
        self.time_offset: int = local_time - server_time

        self.gateway.write_log(f"Linear Server time updated, local offset: {self.time_offset}ms")

    def query_contract(self) -> None:
        """Query available contracts"""
        data: dict = {"security": Security.NONE}

        path: str = "/fapi/v1/exchangeInfo"

        self.add_request(
            method="GET",
            path=path,
            callback=self.on_query_contract,
            data=data
        )
    
    def on_query_contract(self, data: dict, request: Request) -> None:
        """Callback of available contracts query"""
        for d in data["symbols"]:
            base_currency: str = d["baseAsset"]
            quote_currency: str = d["quoteAsset"]
            name: str = f"{base_currency.upper()}/{quote_currency.upper()}"

            pricetick: int = 1
            min_volume: int = 1

            for f in d["filters"]:
                if f["filterType"] == "PRICE_FILTER":
                    pricetick = float(f["tickSize"])
                elif f["filterType"] == "LOT_SIZE":
                    min_volume = float(f["stepSize"])

            contract: ContractData = ContractData(
                symbol=d["symbol"],
                exchange=Exchange.BINANCE,
                name=name,
                pricetick=pricetick,
                size=1,
                min_volume=min_volume,
                product=Product.FUTURES,
                net_position=True,
                history_data=True,
                gateway_name=self.gateway_name,
                stop_supported=True
            )
            self.gateway.on_contract(contract)

            from vnpy_binance.binance_linear_gateway import symbol_contract_map
            symbol_contract_map[contract.symbol] = contract

        self.gateway.write_log("Linear Available contracts data is received")

    def query_history(self, req: HistoryRequest) -> list[BarData]:
        """Query kline history data"""
        history: list[BarData] = []
        limit: int = 1500

        start_time: int = int(datetime.timestamp(req.start))

        while True:
            # Create query parameters
            params: dict = {
                "symbol": req.symbol,
                "interval": INTERVAL_VT2BINANCES[req.interval],
                "limit": limit
            }

            params["startTime"] = start_time * 1000
            path: str = "/fapi/v1/klines"
            if req.end:
                end_time = int(datetime.timestamp(req.end))
                params["endTime"] = end_time * 1000     # Convert to milliseconds

            resp: Response = self.request(
                "GET",
                path=path,
                data={"security": Security.NONE},
                params=params
            )

            # Break the loop if request failed
            if resp.status_code // 100 != 2:
                msg: str = f"Query kline history failed, status code: {resp.status_code}, message: {resp.text}"
                self.gateway.write_log(msg)
                break
            else:
                data: dict = resp.json()
                if not data:
                    msg: str = f"No kline history data is received, start time: {start_time}"
                    self.gateway.write_log(msg)
                    break

                buf: list[BarData] = []

                for row in data:
                    bar: BarData = BarData(
                        symbol=req.symbol,
                        exchange=req.exchange,
                        datetime=generate_datetime(row[0]),
                        interval=req.interval,
                        volume=float(row[5]),
                        turnover=float(row[7]),
                        open_price=float(row[1]),
                        high_price=float(row[2]),
                        low_price=float(row[3]),
                        close_price=float(row[4]),
                        gateway_name=self.gateway_name
                    )
                    bar.extra = {
                        "trade_count": int(row[8]),
                        "active_volume": float(row[9]),
                        "active_turnover": float(row[10]),
                    }
                    buf.append(bar)

                begin: datetime = buf[0].datetime
                end: datetime = buf[-1].datetime

                history.extend(buf)
                msg: str = f"Query kline history finished, {req.symbol} - {req.interval.value}, {begin} - {end}"
                self.gateway.write_log(msg)

                # Break the loop if the latest data received
                if (
                    len(data) < limit
                    or (req.end and end >= req.end)
                ):
                    break

                # Update query start time
                start_dt = bar.datetime + TIMEDELTA_MAP[req.interval]
                start_time = int(datetime.timestamp(start_dt))

            # Wait to meet request flow limit
            sleep(0.5)

        # Remove the unclosed kline
        if history:
            history.pop(-1)

        return history
    

class BinanceInverseRestApi(RestClient):
    """The REST API of BinanceInverseGateway"""

    def __init__(self, gateway: BinancePortfolioMarginGateway) -> None:
        """
        The init method of the api.

        gateway: the parent gateway object for pushing callback data.
        """
        super().__init__()

        self.gateway: BinancePortfolioMarginGateway = gateway
        self.gateway_name: str = gateway.gateway_name

        self.key: str = ""
        self.secret: str = ""

        self.user_stream_key: str = ""
        self.keep_alive_count: int = 0
        self.time_offset: int = 0

        self.order_count: int = 1_000_000
        self.order_prefix: str = ""

    def sign(self, request: Request) -> Request:
        """Standard callback for signing a request"""
        security: Security = request.data["security"]
        if security == Security.NONE:
            request.data = None
            return request

        if request.params:
            path: str = request.path + "?" + urllib.parse.urlencode(request.params)
        else:
            request.params = dict()
            path: str = request.path

        if security == Security.SIGNED:
            timestamp: int = int(time.time() * 1000)

            if self.time_offset > 0:
                timestamp -= abs(self.time_offset)
            elif self.time_offset < 0:
                timestamp += abs(self.time_offset)

            request.params["timestamp"] = timestamp

            query: str = urllib.parse.urlencode(sorted(request.params.items()))
            signature: bytes = hmac.new(
                self.secret,
                query.encode("utf-8"),
                hashlib.sha256
            ).hexdigest()

            query += "&signature={}".format(signature)
            path: str = request.path + "?" + query

        request.path = path
        request.params = {}
        request.data = {}

        # 添加请求头
        headers = {
            "Content-Type": "application/x-www-form-urlencoded",
            "Accept": "application/json",
            "X-MBX-APIKEY": self.key,
            "Connection": "close"
        }

        if security in [Security.SIGNED, Security.API_KEY]:
            request.headers = headers

        return request

    def connect(
        self,
        key: str,
        secret: str,
        server: str,
        proxy_host: str,
        proxy_port: int
    ) -> None:
        """Start server connection"""
        self.key = key
        self.secret = secret.encode()
        self.proxy_port = proxy_port
        self.proxy_host = proxy_host
        self.server = server

        self.order_prefix = datetime.now().strftime("%y%m%d%H%M%S")

        if self.server == "REAL":
            self.init(D_REST_HOST, proxy_host, proxy_port)
        else:
            self.init(D_TESTNET_REST_HOST, proxy_host, proxy_port)

        self.start()

        self.gateway.write_log("Inverse REST API started")

        self.query_time()
        self.query_contract()

    def query_time(self) -> None:
        """Query server time"""
        data: dict = {"security": Security.NONE}

        path: str = "/dapi/v1/time"

        return self.add_request(
            "GET",
            path,
            callback=self.on_query_time,
            data=data
        )

    def on_query_time(self, data: dict, request: Request) -> None:
        """Callback of server time query"""
        local_time: int = int(time.time() * 1000)
        server_time: int = int(data["serverTime"])
        self.time_offset: int = local_time - server_time

        self.gateway.write_log(f"Inverse Server time updated, local offset: {self.time_offset}ms")
    
    def query_contract(self) -> None:
        """Query available contracts"""
        data: dict = {"security": Security.NONE}

        path: str = "/dapi/v1/exchangeInfo"

        self.add_request(
            method="GET",
            path=path,
            callback=self.on_query_contract,
            data=data
        )

    def on_query_contract(self, data: dict, request: Request) -> None:
        """Callback of available contracts query"""
        for d in data["symbols"]:
            base_currency: str = d["baseAsset"]
            quote_currency: str = d["quoteAsset"]
            name: str = f"{base_currency.upper()}/{quote_currency.upper()}"

            pricetick: int = 1
            min_volume: int = 1

            for f in d["filters"]:
                if f["filterType"] == "PRICE_FILTER":
                    pricetick = float(f["tickSize"])
                elif f["filterType"] == "LOT_SIZE":
                    min_volume = float(f["stepSize"])

            contract: ContractData = ContractData(
                symbol=d["symbol"],
                exchange=Exchange.BINANCE,
                name=name,
                pricetick=pricetick,
                size=d["contractSize"],
                min_volume=min_volume,
                product=Product.FUTURES,
                net_position=True,
                history_data=True,
                gateway_name=self.gateway_name,
                stop_supported=True
            )
            self.gateway.on_contract(contract)

            from vnpy_binance.binance_inverse_gateway import symbol_contract_map
            symbol_contract_map[contract.symbol] = contract

        self.gateway.write_log("Inverse Available contracts data is received")

    def query_history(self, req: HistoryRequest) -> list[BarData]:
            """Query kline history data"""
            history: list[BarData] = []
            limit: int = 1500

            end_time: int = int(datetime.timestamp(req.end))

            while True:
                # Create query parameters
                params: dict = {
                    "symbol": req.symbol,
                    "interval": INTERVAL_VT2BINANCES[req.interval],
                    "limit": limit
                }

                params["endTime"] = end_time * 1000
                path: str = "/dapi/v1/klines"
                # if req.start:
                    # start_time = int(datetime.timestamp(req.start))
                    # params["startTime"] = start_time * 1000     # Convert to milliseconds

                resp: Response = self.request(
                    "GET",
                    path=path,
                    data={"security": Security.NONE},
                    params=params
                )

                # Break the loop if request failed
                if resp.status_code // 100 != 2:
                    msg: str = f"Query kline history failed, status code: {resp.status_code}, message: {resp.text}"
                    self.gateway.write_log(msg)
                    break
                else:
                    data: dict = resp.json()
                    if not data:
                        msg: str = f"No kline history data is received, start time: {start_time}"
                        self.gateway.write_log(msg)
                        break

                    buf: list[BarData] = []

                    for row in data:
                        bar: BarData = BarData(
                            symbol=req.symbol,
                            exchange=req.exchange,
                            datetime=generate_datetime(row[0]),
                            interval=req.interval,
                            volume=float(row[5]),
                            turnover=float(row[7]),
                            open_price=float(row[1]),
                            high_price=float(row[2]),
                            low_price=float(row[3]),
                            close_price=float(row[4]),
                            gateway_name=self.gateway_name
                        )
                        bar.extra = {
                            "trade_count": int(row[8]),
                            "active_volume": float(row[9]),
                            "active_turnover": float(row[10]),
                        }
                        buf.append(bar)

                    begin: datetime = buf[0].datetime
                    end: datetime = buf[-1].datetime

                    buf = list(reversed(buf))
                    history.extend(buf)
                    msg: str = f"Query kline history finished, {req.symbol} - {req.interval.value}, {begin} - {end}"
                    self.gateway.write_log(msg)

                    # Break the loop if the latest data received
                    if (
                        len(data) < limit
                        or (req.start and begin <= req.start)
                    ):
                        break

                    # Update query start time
                    end_dt = begin - TIMEDELTA_MAP[req.interval]
                    end_time = int(datetime.timestamp(end_dt))

                # Wait to meet request flow limit
                sleep(0.2)

            # Reverse the entire list
            history = list(reversed(history))

            # Remove the unclosed kline
            if history:
                history.pop(-1)

            return history


class PortfolioMarginTradeWebsocketApi(WebsocketClient):
    """The trade websocket API of PortfolioMarginGateway"""

    def __init__(self, gateway: BinancePortfolioMarginGateway) -> None:
        """
        The init method of the api.
        gateway: the parent gateway object for pushing callback data.
        """
        super().__init__()

        self.gateway: BinancePortfolioMarginGateway = gateway
        self.gateway_name: str = gateway.gateway_name

    def connect(self, url: str, proxy_host: str, proxy_port: int) -> None:
        """Start server connection"""
        
        self.init(url, proxy_host, proxy_port, receive_timeout=WEBSOCKET_TIMEOUT)
        self.start()

    def on_connected(self) -> None:
        """Callback when websocket is connected"""
        self.gateway.write_log("Portfolio Margin Trade Websocket API connected")

    def on_packet(self, packet: dict) -> None:
        """Callback of data received"""

        if packet["e"] == "ACCOUNT_UPDATE":
            self.on_account(packet)
        elif packet["e"] == "ORDER_TRADE_UPDATE":
            self.on_order(packet)
        elif packet["e"] == "listenKeyExpired":
            self.on_listen_key_expired()

    def on_error(self, e: Exception) -> None:
        """
        Callback when exception raised.
        """
        self.gateway.write_log(f"Profolio Margin Trade Websocket API exception: {e}")

    def on_listen_key_expired(self) -> None:
        """Callback of listen key expired"""
        self.gateway.write_log("Listen key is expired")

    def on_account(self, packet: dict) -> None:
        """Callback of account update"""

        '''这里的接口在返回账户信息时，只传送余额的变化量，暂不使用'''
        # for acc_data in packet["a"]["B"]:
        #     account: AccountData = AccountData(
        #         accountid=acc_data["a"],
        #         balance=float(acc_data["wb"]),
        #         frozen=float(acc_data["wb"]) - float(acc_data["cw"]),
        #         gateway_name=self.gateway_name
        #     )
        #     self.gateway.on_account(account)

        for pos_data in packet["a"]["P"]:
            volume = float(pos_data["pa"])
            position: PositionData = PositionData(
                symbol=pos_data["s"],
                exchange=Exchange.BINANCE,
                direction=Direction.NET,
                volume=abs(volume),
                price=float(pos_data["ep"]),
                pnl=float(pos_data["up"]),
                gateway_name=self.gateway_name,
            )
            self.gateway.on_position(position)

    def on_order(self, packet: dict) -> None:
        """Callback of order and trade update"""
        d: dict = packet["o"]
        key: tuple[str, str] = (d["o"], d["f"])
        order_type: OrderType = ORDERTYPE_BINANCES2VT.get(key, None)
        if not order_type:
            return

        order: OrderData = OrderData(
            symbol=d["s"],
            exchange=Exchange.BINANCE,
            orderid=d["c"],
            type=order_type,
            direction=DIRECTION_BINANCES2VT[d["S"]],
            price=float(d["p"]),
            volume=float(d["q"]),
            traded=float(d["z"]),
            status=STATUS_BINANCES2VT[d["X"]],
            datetime=generate_datetime(d["T"]),
            gateway_name=self.gateway_name
        )
        self.gateway.on_order(order)

        # Push trade data
        if not float(d["l"]):
            return

        trade: TradeData = TradeData(
            symbol=order.symbol,
            exchange=order.exchange,
            orderid=order.orderid,
            tradeid=str(d["t"]),
            direction=order.direction,
            price=float(d["L"]),
            volume=float(d["l"]),
            datetime=generate_datetime(d["T"]),
            gateway_name=self.gateway_name,
        )
        self.gateway.on_trade(trade)

    def on_disconnected(self, status_code: int, msg: str) -> None:
        """Callback when websocket is disconnected"""
        self.gateway.write_log(f"Trade Websocket API disconnected, status: {status_code}, message: {msg}")

    def on_error(self, e: Exception) -> None:
        """Callback when websocket has error"""
        self.gateway.write_log(f"Trade Websocket API error: {e}")


def generate_datetime(timestamp: float) -> datetime:
    """
    Generate datetime object from timestamp.
    """
    dt: datetime = datetime.fromtimestamp(timestamp / 1000, tz=UTC_TZ)
    return dt


def format_float(f: float) -> str:
    """
    Format float number to string.
    """
    return format_float_positional(f, trim="-")