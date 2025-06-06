import logging
import os


def get_files(folder_path):
    from os import scandir, getcwd

    def ls(folder_path=getcwd()):
        return [arch.name for arch in scandir(folder_path) if arch.is_file()]

    return ls(folder_path)


class Log:
    def __init__(self):
        pass

    # Get an instance of a logger
    @staticmethod
    def get_logger(name=None):
        """
            Create a Logging object
        """
        try:
            if name:
                logger = logging.getLogger(name)
                if not logger.hasHandlers():
                    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')

                    file_handler = logging.FileHandler(os.path.join(os.path.abspath('./'), '%s.log' % name), mode='w+',
                                                       encoding='utf-8')
                    file_handler.setFormatter(formatter)

                    stream_handler = logging.StreamHandler()
                    stream_handler.setFormatter(formatter)

                    logger.setLevel(logging.INFO)
                    logger.addHandler(file_handler)
                    logger.addHandler(stream_handler)
            else:
                logger = logging.getLogger('output')
                if not logger.hasHandlers():
                    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')

                    file_handler = logging.FileHandler(os.path.join(os.path.abspath('./'), 'output.log'), mode='w+')
                    file_handler.setFormatter(formatter)

                    stream_handler = logging.StreamHandler()
                    stream_handler.setFormatter(formatter)

                    logger.setLevel(logging.INFO)
                    logger.addHandler(file_handler)
                    logger.addHandler(stream_handler)

            return logger
        except Exception as e:
            raise e

