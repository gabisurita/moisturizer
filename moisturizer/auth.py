from pyramid.authentication import BasicAuthAuthenticationPolicy
from pyramid.authorization import ACLAuthorizationPolicy
from pyramid.httpexceptions import HTTPUnauthorized

from moisturizer.exceptions import AuthenticationError, format_http_error
from moisturizer.models import UserModel


def check_user(id, key, request):
    if id is not None:
        try:
            user = UserModel.objects.get(id=id)
            setattr(request, 'user', user)

        except UserModel.DoesNotExist:
            raise format_http_error(HTTPUnauthorized, AuthenticationError())

        if str(user.api_key) == key:
            return [user.role]
        elif user.check_password(key):
            return [user.role]

        raise format_http_error(HTTPUnauthorized, AuthenticationError())


def includeme(config):
    authn_policy = BasicAuthAuthenticationPolicy(check_user)
    config.set_authentication_policy(authn_policy)
    config.set_authorization_policy(ACLAuthorizationPolicy())
