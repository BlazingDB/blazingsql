import os
import yaml

class configTest():
    worder = None
    use_percentage = None
    acceptable_difference = None
    orderby = None
    print_result = None
    data_types = None

class testInfo():
    def __init__(self, name, configFile, default):
        self.name = name
        self.configFile = configFile
        self.data = None
        self.defaultConfig = default
        self.config = configTest()

        self.__loadConfig()

    def __loadConfig(self):
        if os.path.isfile(self.configFile):
            with open(self.configFile, 'r') as stream:
                self.data = yaml.load(stream)[self.name]

        if "config" in self.data:
            if "worder" in self.data["config"]:
                self.config.worder = self.data["config"]["worder"]
            if "use_percentage" in self.data["config"]:
                self.config.use_percentage = self.data["config"]["use_percentage"]
            if "acceptable_difference" in self.data["config"]:
                self.config.acceptable_difference = self.data["config"]["acceptable_difference"]
            if "orderby" in self.data["config"]:
                self.config.orderby = self.data["config"]["orderby"]
            if "print_result" in self.data["config"]:
                self.config.print_result = self.data["config"]["print_result"]
            if "data_types" in self.data["config"]:
                self.config.data_types = self.data["config"]["data_types"]

    def run(self):
        a = 10
