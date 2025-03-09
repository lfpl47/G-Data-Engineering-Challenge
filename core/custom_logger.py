import logging

class CustomLogger:
    """
    Clase CustomLogger que encapsula la configuración del logging.
    Proporciona métodos para registrar mensajes en diferentes niveles: debug, info, warning y error.
    """
    def __init__(self, name: str):
        """
        Inicializa el logger con un nombre específico y configura el formato y el nivel de logging.
        :param name: Nombre del logger, típicamente el nombre de la clase o módulo.
        """
        self.logger = logging.getLogger(name)
        self.logger.setLevel(logging.DEBUG)

        ch = logging.StreamHandler()
        ch.setLevel(logging.DEBUG)

        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        ch.setFormatter(formatter)

        if not self.logger.handlers:
            self.logger.addHandler(ch)

    def debug(self, message: str):
        """
        Registra un mensaje a nivel DEBUG.
        :param message: Mensaje a registrar.
        """
        self.logger.debug(message)

    def info(self, message: str):
        """
        Registra un mensaje a nivel INFO.
        :param message: Mensaje a registrar.
        """
        self.logger.info(message)

    def warning(self, message: str):
        """
        Registra un mensaje a nivel WARNING.
        :param message: Mensaje a registrar.
        """
        self.logger.warning(message)

    def error(self, message: str):
        """
        Registra un mensaje a nivel ERROR.
        :param message: Mensaje a registrar.
        """
        self.logger.error(message)
