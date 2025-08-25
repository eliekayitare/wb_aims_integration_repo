from rest_framework.pagination import PageNumberPagination
from rest_framework.response import Response

class FlexiblePageSizePagination(PageNumberPagination):
    page_size_query_param = 'page_size'
    page_size = 10  # Default page size
    # max_page_size = 100  # Maximum allowed page size
    max_page_size = None 
    
    def get_paginated_response(self, data):
        return Response({
            'count': self.page.paginator.count,
            'total_pages': self.page.paginator.num_pages,
            'current_page': self.page.number,
            'page_size': self.get_page_size(self.request),
            'next': self.get_next_link(),
            'previous': self.get_previous_link(),
            'results': data
        })