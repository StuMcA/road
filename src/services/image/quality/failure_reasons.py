from enum import Enum


class ImageFailureReason(Enum):
    """Enumeration of possible image quality failure reasons"""

    TOO_BLURRY = "too_blurry"
    TOO_DARK = "too_dark"
    TOO_BRIGHT = "too_bright"
    RESOLUTION_TOO_SMALL = "resolution_too_small"
    INSUFFICIENT_ROAD_SURFACE = "insufficient_road_surface"
    FILE_NOT_FOUND = "file_not_found"
    PROCESSING_ERROR = "processing_error"

    @property
    def display_message(self) -> str:
        """Human-readable failure message"""
        messages = {
            self.TOO_BLURRY: "Image is too blurry for analysis",
            self.TOO_DARK: "Image is too dark (underexposed)",
            self.TOO_BRIGHT: "Image is too bright (overexposed)",
            self.RESOLUTION_TOO_SMALL: "Image resolution is too small",
            self.INSUFFICIENT_ROAD_SURFACE: "Insufficient road surface visible in image",
            self.FILE_NOT_FOUND: "Image file not found",
            self.PROCESSING_ERROR: "Error occurred during image processing",
        }
        return messages[self]

    @classmethod
    def get_display_messages(cls, reasons: list["ImageFailureReason"]) -> list[str]:
        """Get display messages for a list of failure reasons"""
        return [reason.display_message for reason in reasons]
