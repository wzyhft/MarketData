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

# todo: add snapshot and assset info
# todo: file may be too large if run forever?
# todo: custom file name and path in runtime?
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
            "filename": "md_depth.log",
        },
    },
    "root": {
        "handlers": ["fileHandler"],
        "level": logging.INFO
    }
})

logger = logging.getLogger("__name__")


class MarketDataWorker:
    def __init__(self, url, symbol, file_prefix):
        super(MarketDataWorker, self).__init__()
        self.url = url
        self.symbol = symbol
        self.now = datetime.datetime.utcnow()
        self.next_day_ms = (self.now + datetime.timedelta(days=1)).replace(tzinfo=datetime.timezone.utc,
                                                                           hour=0, minute=0, second=0,
                                                                           microsecond=0).timestamp() * 1000.0
        self.file_prefix = file_prefix
        self.filename = f"{self.file_prefix}_{self.now.strftime('%Y%m%d')}"
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

        logger.info(f"MarketDataWorker starts on UTC {self.now},\nsymbol:{self.symbol},\nwritting in {self.filename},"
                    f" pid: {os.getpid()}, will do compress and upload on {self.next_day_ms}")
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
            time.sleep(random.randint(0, 1000))
            with open(os.path.join('data', filename), mode="rb") as fin, open(os.path.join('data', f"{filename}.zlib"),
                                                                              mode="wb") as fout:
                data = fin.read()
                compress = zlib.compressobj(level=zlib.Z_BEST_COMPRESSION, wbits=-15, memLevel=2)
                compressed_data = compress.compress(data)
                compressed_data += compress.flush()
                logger.info(f"Original size before compress: {sys.getsizeof(data)}")
                logger.info(f"Compressed size: {sys.getsizeof(compressed_data)}")
                fout.write(compressed_data)
            try:
                self.upload_to_drive(f"data/{filename}.zlib")
            except Exception as e:
                logger.error(f"uploading file to drive got exception: {repr(e)}")
            finally:
                logger.info("upload to drive done")

        thread.start_new_thread(compress_and_upload, (self.filename,))

        self.now = datetime.datetime.fromtimestamp(self.next_day_ms / 1000)
        self.next_day_ms = (self.now + datetime.timedelta(days=1)).replace(hour=0, minute=0, second=0,
                                                                           microsecond=0).timestamp() * 1000.0
        self.filename = f"{self.file_prefix}_{self.now.strftime('%Y%m%d')}"
        logger.info(f"next_day_ms switched to {self.next_day_ms} and filename switched to {self.filename}")

    def write_file(self, data):
        with open(os.path.join('data', self.filename), 'a') as outfile:
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
        # todo: a better way to reconnect when connection down
        if isinstance(error, Exception) and str(error).startswith("Connection to remote host was lost"):
            self.need_reconnect = True

    def on_close(self, ws, closeCode, closeMsg):
        logger.info(f"[{closeCode}]### closed ### {closeMsg}")
        # todo: I don't know why I got this shit
        # "[1001]### closed ### CloudFlare WebSocket proxy restarting"
        if closeCode == 1001:
            self.need_reconnect = True
            
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
    file_prefix = ""
    grp = 4
    if os.path.isfile(config):
        ascdexCfg = load_config(config)['ascendex']
        wss_url = ascdexCfg['wss']
        grp = ascdexCfg['group']
        symbol = ascdexCfg['symbol']
        file_prefix = ascdexCfg['file_prefix']

    url = f"{wss_url}/{grp}/{ROUTE_PREFIX}/stream"
    # logger.info(f"connecting to {url}, symbol {symbol}")

    pid = daemon.pidfile.PIDLockFile(f"/tmp/{file_prefix}.pid")
    if pid.is_locked():
        raise Exception("Process is already started")

    # https://blog.csdn.net/liuxingen/article/details/71169502
    dc = daemon.DaemonContext(working_directory=os.getcwd(),
                              umask=0o002,  # prevent others write
                              stdout=open("./log/daemonout.log", "a"),
                              stderr=open("./log/daemonerr.log", "a"),
                              pidfile=pid,
                              files_preserve=[logging.root.handlers[0].stream])

    with dc:
        marketDataWorker = MarketDataWorker(url, symbol, file_prefix)
        marketDataWorker.run()

# todo: limit memory usage
if __name__ == "__main__":
    if not os.path.exists('log'):
        os.makedirs('log')
    if not os.path.exists('data'):
        os.makedirs('data')
    run()
