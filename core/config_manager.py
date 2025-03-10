import yaml

class ConfigManager:
    """
    ConfigManager class is responsible for loading and accessing the project configuration.
    It allows reading a YAML file and returning the configuration as a dictionary.
    """
    
    @staticmethod
    def load_config(config_path: str = "config.yaml") -> dict:
        """
        Loads the configuration from a YAML file.
        :param config_path: Path to the YAML file (default is "config.yaml").
        :return: Dictionary with the loaded configuration.
        :raises FileNotFoundError: If the file is not found.
        :raises yaml.YAMLError: If an error occurs while parsing the YAML.
        """
        with open(config_path, "r") as file:
            config = yaml.safe_load(file)
        return config
