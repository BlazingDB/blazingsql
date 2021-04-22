import os
import yaml

class configTest():
    worder = None
    use_percentage = None
    acceptable_difference = None
    orderby = None
    print_result = None
    data_types = None

class testRunner():
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

    def __loadConfigQuery(self, name):
        configQuery = configTest()
        if "config" in self.data["listTest"][name]:
            if "worder" in self.data["listTest"][name]["config"]:
                configQuery.worder = self.data["config"]["worder"]
            if "use_percentage" in self.data["listTest"][name]["config"]:
                configQuery.use_percentage = self.data["listTest"][name]["config"]["use_percentage"]
            if "acceptable_difference" in self.data["listTest"][name]["config"]:
                configQuery.acceptable_difference = self.data["listTest"][name]["config"]["acceptable_difference"]
            if "orderby" in self.data["listTest"][name]["config"]:
                configQuery.orderby = self.data["listTest"][name]["config"]["orderby"]
            if "print_result" in self.data["listTest"][name]["config"]:
                configQuery.print_result = self.data["listTest"][name]["config"]["print_result"]
            if "data_types" in self.data["listTest"][name]["config"]:
                configQuery.data_types = self.data["listTest"][name]["config"]["data_types"]

        return configQuery

    def run(self):
        a = 10
