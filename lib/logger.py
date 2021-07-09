class Log4J:
    def __init__(self, spark):
        log4j = spark._jvm.org.apache.log4j
        module_name = 'self.learning.sparkstreaming.practice'
        conf = spark.sparkContext.getConf()
        app_name = conf.get('spark.app.name')

        self.logger = log4j.LogManager.getLogger(module_name + '.' + app_name)

    def info(self, message):
        self.logger.info(message)

    def warn(self, message):
        self.logger.warn(message)

    def error(self, message):
        self.logger.error(message)

    def debug(self, message):
        self.logger.debug(message)

    def fatal(self, message):
        self.logger.fatal(message)