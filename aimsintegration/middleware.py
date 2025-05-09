import logging
from django.http import HttpResponse

logger = logging.getLogger(__name__)

class ErrorHandlingMiddleware:
    def __init__(self, get_response):
        self.get_response = get_response
        
    def __call__(self, request):
        return self.get_response(request)
        
    def process_exception(self, request, exception):
        # Log the error
        logger.error(f"Unhandled exception: {str(exception)}", exc_info=True)
        
        # Return a generic error response
        return HttpResponse("An error occurred. Our team has been notified.", status=500)