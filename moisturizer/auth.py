from pyramid.authentication import BasicAuthAuthenticationPolicy
from pyramid.authorization import ACLAuthorizationPolicy
from pyramid.httpexceptions import HTTPUnauthorized

from moisturizer.exceptions import AuthenticationError, format_http_error
from moisturizer.models import UserModel


def check_user(id, password, request):
    if id is not None:
        try:
            user = UserModel.objects.get(id=id)
            if not user.check_password(password):
                raise AuthenticationError()

        except UserModel.DoesNotExist:
            raise format_http_error(HTTPUnauthorized, AuthenticationError())

        except AuthenticationError as e:
            raise format_http_error(HTTPUnauthorized, e)

        setattr(request, 'user', user)


def includeme(config):
    authn_policy = BasicAuthAuthenticationPolicy(check_user)
    config.set_authentication_policy(authn_policy)
    config.set_authorization_policy(ACLAuthorizationPolicy())
