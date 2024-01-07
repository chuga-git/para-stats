class RateLimitError(Exception):
    def __str__(self) -> str:
        return "Exceeded API rate limit."
    
class RoundNotFoundError(Exception):
    def __str__(self) -> str:
        return "HTTP response status 404: round not found or is still ongoing."