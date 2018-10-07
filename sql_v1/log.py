import logging


class Log:
    def write(self, statement, rant='info'):
        format = logging.Formatter("%(levelname)s %(asctime)s %(message)s")

        # 输出到文件
        applog_hand = logging.FileHandler("database.log")
        applog_hand.setFormatter(format)

        # 创建logger对象，并定义相关日志属性
        app_log = logging.getLogger("app")
        app_log.setLevel(logging.INFO)
        app_log.addHandler(applog_hand)  # logger对象继承处理器

        logging.getLogger("app.net").setLevel(logging.ERROR)

        app_log = logging.getLogger("app")
        if rant == 'info':
            app_log.info(statement)
        elif rant == 'error':
            app_log.error(statement)