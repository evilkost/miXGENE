from django.http import HttpResponse
from django.template import RequestContext, loader
from django.shortcuts import redirect

from django.contrib.auth import authenticate, login, logout

def index(request):
    template = loader.get_template('index.html')
    context = RequestContext(request, {})
    return HttpResponse(template.render(context))

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