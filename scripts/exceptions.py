class WatermarkInvalidException(Exception):
    def __init__(self, message="Watermark Invalid: Incoming data is older than already existing one"):
        super().__init__(message)
    pass