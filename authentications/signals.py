from django.db.models.signals import post_migrate
from django.dispatch import receiver
from .models import Role

@receiver(post_migrate)
def create_default_roles(sender, **kwargs):
    roles = ['staff', 'admin']  # Add any other roles you need
    for role_name in roles:
        Role.objects.get_or_create(name=role_name)
