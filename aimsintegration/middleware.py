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

class MediaFileMiddleware:
    def __init__(self, get_response):
        self.get_response = get_response

    def __call__(self, request):
        response = self.get_response(request)
        
        # Add headers for media files
        if request.path.startswith('/media/'):
            response['X-Content-Type-Options'] = 'nosniff'
            response['Content-Disposition'] = 'inline'
            if request.path.endswith('.pdf'):
                response['Content-Type'] = 'application/pdf'
                # Remove X-Frame-Options to allow iframe embedding
                if 'X-Frame-Options' in response:
                    del response['X-Frame-Options']
        
        return response