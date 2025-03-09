import yaml

class ConfigManager:
    """
    Clase ConfigManager para gestionar la carga y acceso a la configuración del proyecto.
    Permite leer un archivo YAML y retornar la configuración como un diccionario.
    """
    
    @staticmethod
    def load_config(config_path: str = "config.yaml") -> dict:
        """
        Carga la configuración desde un archivo YAML.
        :param config_path: Ruta al archivo YAML (por defecto "config.yaml").
        :return: Diccionario con la configuración cargada.
        :raises FileNotFoundError: Si el archivo no se encuentra.
        :raises yaml.YAMLError: Si ocurre un error al parsear el YAML.
        """
        with open(config_path, "r") as file:
            config = yaml.safe_load(file)
        return config
