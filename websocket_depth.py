# Please use Python 3.6 or higher
# * installation: pip install websocket_client
# * pip install python-daemon for deamon

import datetime
import zlib
import click
import websocket
try:
    import thread
except ImportError:
    import _thread as thread
import daemon
import daemon.pidfile
import logging
import logging.config
# Local imports 
from util import *

# set logger
class UTCFormatter(logging.Formatter):
    converter = time.gmtime
logging.config.dictConfig({
    "version": 1,
    "formatters": {
        "customFormat": {
            '()': UTCFormatter,
            "format": "%(asctime)s %(filename)-10s %(funcName)-12s:%(lineno)3d %(levelname)s %(message)s",
        },
    },
    "handlers": {
        "fileHandler": {
            "class": "logging.FileHandler",
            "formatter": "customFormat",
            "filename": "./md_depth.log",
        },
    },
    "root": {
        "handlers": ["fileHandler"],
        "level": logging.INFO
    }
})

logger = logging.getLogger("__name__")


class MarketDataWorker:
    def __init__(self, url, symbol):
        super(MarketDataWorker, self).__init__()
        self.url = url
        self.symbol = symbol
        self.now = datetime.datetime.utcnow()
        self.next_day_ms = (self.now + datetime.timedelta(days=1)).replace(tzinfo=datetime.timezone.utc,
                                                                           hour=0, minute=0, second=0,
                                                                           microsecond=0).timestamp() * 1000.0
        self.filename = f"{self.now.strftime('%Y%m%d')}_depth_raw_json"
        self.ws = None
        self.need_reconnect = False
        self.reconnect_count = 5

    def run(self):
        websocket.enableTrace(False)
        self.ws = websocket.WebSocketApp(self.url,
                                         on_open=self.on_open,
                                         on_message=self.on_message,
                                         on_error=self.on_error,
                                         on_close=self.on_close)

        logger.info(f"MarketDataWorker starts on UTC {self.now} for symbol:{self.symbol}, writting in {self.filename},"
                     f" will do compress and upload on {self.next_day_ms}")
        self.ws.run_forever()

    def upload_to_drive(self, file):
        from pydrive.auth import GoogleAuth
        from pydrive.drive import GoogleDrive
        gauth = GoogleAuth()
        gauth.CommandLineAuth()
        drive = GoogleDrive(gauth)

        upload_file_list = [file]
        for upload_file in upload_file_list:
            gfile = drive.CreateFile({'parents': [{'id': '13DXRAaACLP7l9m-VMIjdmSVqPSs353gL'}]})
            # Read file and set it as the content of this instance.
            gfile.SetContentFile(upload_file)
            gfile.Upload()  # Upload the file.

    def update_date(self):
        def compress_and_upload(filename):
            with open(filename, mode="rb") as fin, open(f"{filename}.zlib", mode="wb") as fout:
                data = fin.read()
                compressed_data = zlib.compress(data, zlib.Z_BEST_COMPRESSION)
                logger.debug(f"Original size before compress: {sys.getsizeof(data)}")
                logger.debug(f"Compressed size: {sys.getsizeof(compressed_data)}")
                fout.write(compressed_data)
            try:
                self.upload_to_drive(f"{filename}.zlib")
            except Exception as e:
                logger.error(f"uploading file to drive got exception: {repr(e)}")
            finally:
                logger.error("upload to drive failed failed")

        thread.start_new_thread(compress_and_upload, (self.filename,))

        self.now = datetime.datetime.fromtimestamp(self.next_day_ms / 1000)
        self.next_day_ms = (self.now + datetime.timedelta(days=1)).replace(hour=0, minute=0, second=0,
                                                                           microsecond=0).timestamp() * 1000.0
        self.filename = f"{self.now.strftime('%Y%m%d')}_depth_raw_json"

    def write_file(self, data):
        with open(self.filename, 'a') as outfile:
            outfile.write(json.dumps(data))
            outfile.write("\u000a")

    def send(self, msg):
        o = json.dumps(msg)
        logger.info(f"sending {str(o)}")
        self.ws.send(o)

    def send_pong(self):
        sub = dict(op="pong")
        self.send(sub)

    def sub_depth(self):
        sub = dict(op="sub", ch=f"depth:{self.symbol}")
        self.send(sub)

    # Get full depth of snapshot as init, then use depth to update
    # This has a high latency response for about 1000-2000 ms, so
    # there maybe some issues when we reconstruct our book.
    def req_depth_snapshot(self):
        req = dict(op="req", action=f"depth-snapshot", args=dict(symbol=self.symbol))
        self.send(req)

    # https://ascendex.github.io/ascendex-pro-api/#ws-orderbook-snapshot
    def on_message(self, ws, message):
        obj = json.loads(message)
        msg_type = obj['m']
        if msg_type == 'depth':
            self.write_file(obj)
            exchange_ts = obj["data"]["ts"]
            if exchange_ts > self.next_day_ms:
                self.update_date()
        elif msg_type == "ping":
            self.send_pong()
        else:
            logger.info(f"ignore message [{msg_type}]")

    def on_error(self, ws, error):
        logger.error(f"websocket-client on error {repr(error)}")
        #todo: a better way to reconnect when connection down
        if isinstance(error, Exception) and str(error).startswith("Connection to remote host was lost"):
            self.need_reconnect = True

    def on_close(self, ws, closeCode, closeMsg):
        logger.info(f"[{closeCode}]### closed ### {closeMsg}")
        if self.need_reconnect and self.reconnect_count > 0:
            self.reconnect_count -= 1
            self.run()

    def on_open(self, ws):
        self.need_reconnect = False
        self.reconnect_count = 5
        def run(*args):
            self.sub_depth()

        thread.start_new_thread(run, ())


@click.command()
@click.option("--symbol", type=str, default="BTC/USDT")
@click.option("--config", type=str, default="config.json")
def run(symbol, config):
    if config is None:
        config = os.path.join(os.path.dirname(os.path.abspath(__file__)), ".", "config.json")
        logger.info(f"Config file is not specified, use {config}")
    wss_url = "wss://ascendex.com:443"
    grp = 4
    if os.path.isfile(config):
        ascdexCfg = load_config(config)['ascendex']
        wss_url = ascdexCfg['wss']
        grp = ascdexCfg['group']
        symbol = ascdexCfg['symbol']

    url = f"{wss_url}/{grp}/{ROUTE_PREFIX}/stream"
    #logger.info(f"connecting to {url}, symbol {symbol}")

    pid =  daemon.pidfile.PIDLockFile("/tmp/md_depth.pid")
    if pid.is_locked():
        raise Exception("Process is already started")

    # https://blog.csdn.net/liuxingen/article/details/71169502
    dc = daemon.DaemonContext(working_directory=os.getcwd(),
                              umask=0o002, #prevent others write
                              stdout=open("./daemonout.log", "a"),
                              stderr=open("./daemonerr.log", "a"),
                              pidfile=pid,
                              files_preserve=[logging.root.handlers[0].stream])

    with dc:
        marketDataWorker = MarketDataWorker(url, symbol)
        marketDataWorker.run()


if __name__ == "__main__":
    run()
