from django.http import HttpResponse
from django.template import RequestContext, loader
from django.shortcuts import redirect

from django.views.decorators.debug import sensitive_post_parameters
from django.views.decorators.cache import never_cache
from django.views.decorators.csrf import csrf_protect

from django.contrib.auth import authenticate, login, logout
from django.contrib.auth.models import User
from django.contrib.auth.decorators import login_required

from webapp.models import Experiment, WorkflowLayout
from workflow.tasks import exc_task, set_exp_status
from workflow.layout import write_result
from mixgene.util import dyn_import


def index(request):
    template = loader.get_template('index.html')
    context = RequestContext(request, {
        "next":"/",
    })
    return HttpResponse(template.render(context))


def about(request):
    template = loader.get_template('about.html')
    context = RequestContext(request, {
        "next":"/",
    })
    return HttpResponse(template.render(context))

def contact(request):
    template = loader.get_template('contact.html')
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


@login_required(login_url='/auth/login/')
def experiments(request):
    template = loader.get_template('experiments.html')
    context = RequestContext(request, {
        "exps": Experiment.objects.filter(author=request.user),
        "next": "/experiments",
        "exp_page_active": True,
    })
    return HttpResponse(template.render(context))

@login_required(login_url='/auth/login/')
def exp_details(request, exp_id):
    exp = Experiment.objects.get(e_id = exp_id)

    layout = exp.workflow
    wfl_class = dyn_import(layout.wfl_class)
    wf = wfl_class()

    template = loader.get_template(wf.template_result)
    context = RequestContext(request, {
        "exp_page_active": True,

        "exp": exp,
        "layout": layout,
    })
    return HttpResponse(template.render(context))



@login_required(login_url='/auth/login/')
def add_experiment(request):
    template = loader.get_template('add_experiment.html')
    context = RequestContext(request, {
        "next":"/add_experiment",
        "exp_add_page_active": True,

        "all_layouts": WorkflowLayout.objects.all()
    })
    return HttpResponse(template.render(context))

@login_required(login_url='auth/login/')
def create_experiment(request):
    layout_id = int(request.POST['id_wfl'])

    layout = WorkflowLayout.objects.get(w_id=layout_id)
    wfl_class = dyn_import(layout.wfl_class)
    wf = wfl_class()

    template = loader.get_template(wf.template)
    context = RequestContext(request, {
        #"next":"/add_experiment",
        "exp_add_page_active": True,

        "layout_id": layout_id,
    })
    return HttpResponse(template.render(context))


@login_required(login_url='auth/login/')
def create_exp_instance(request):
    layout_id = int(request.POST['id_wfl'])

    layout = WorkflowLayout.objects.get(w_id=layout_id)
    wfl_class = dyn_import(layout.wfl_class)
    wf = wfl_class()

    main_task, ctx = wf.get_workflow(request)

    exp = Experiment(
        author=request.user,
        workflow=layout,
        wfl_setup=ctx
    )
    exp.save()

    ctx['exp_id'] = exp.e_id
    exc_task.s(ctx, main_task, set_exp_status).apply_async()


    return redirect("/")


