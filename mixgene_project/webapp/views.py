from django.http import HttpResponse
from django.template import RequestContext, loader
from django.shortcuts import redirect

from django.views.decorators.debug import sensitive_post_parameters
from django.views.decorators.cache import never_cache
from django.views.decorators.csrf import csrf_protect

from django.contrib.auth import authenticate, login, logout
from django.contrib.auth.models import User

def index(request):
    template = loader.get_template('index.html')
    context = RequestContext(request, {
        "next":"/",
    })
    return HttpResponse(template.render(context))

@csrf_protect
@never_cache
def create_user(request):
    if 'username' not in request.POST:
        template = loader.get_template('auth/user_creation.html')
        context = RequestContext(request, {
            "next":"/",
        })
        return HttpResponse(template.render(context))

    else:        
        username = request.POST['username']
        email = request.POST['email']
        password = request.POST['password1']
        password2 = request.POST['password2']
        
        # Add nice warning for UI
        if password != password2:
            print "unmatched passwords"
            pass
            #TODO: redirect to create user page with warning, keep user name, flush passwords
        else:
            #TODO: check whether we already have user with the same name or email
            #TODO: add (or not) email validation ? captcha?
            
            user = User.objects.create_user(username, email, password)
            user = authenticate(username=username, password=password)
            login(request, user)
        
        # add user created page, or auto
        return redirect("/")

    
"""
def login(request):
    username = request.POST['username']
    password = request.POST['password']
    user = authenticate(username=username, password=password)
    if user is not None:
        if user.is_active:
            login(request, user)
            return redirecrt(request.path)
        else:
            # Return a 'disabled account' error message
            return redirecrt(request.path)
    else:
        return redirecrt(request.path)
        # Return an 'invalid login' error message.
"""
