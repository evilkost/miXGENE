import cPickle as pickle

from django.http import HttpResponse
from django.template import RequestContext, loader
from django.shortcuts import redirect

from django.views.decorators.debug import sensitive_post_parameters
from django.views.decorators.cache import never_cache
from django.views.decorators.csrf import csrf_protect

from django.contrib.auth import authenticate, login, logout
from django.contrib.auth.models import User
from django.contrib.auth.decorators import login_required

from webapp.models import Experiment, WorkflowLayout, UploadedData
from webapp.forms import UploadForm
from workflow.tasks import exc_task, set_exp_status, CTX_STORE_REDIS_PREFIX
from workflow.layout import write_result
from mixgene.util import dyn_import
from mixgene.util import get_redis_instance


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
def upload_data(request):
    if request.method == "POST":
        form = UploadForm(request.POST, request.FILES)
        if form.is_valid():
            exp_id=form.cleaned_data['exp_id']
            exp = Experiment.objects.get(e_id=exp_id)
            # check 1: does this expirement use such variable

            layout = exp.workflow
            wfl_class = dyn_import(layout.wfl_class)
            wf = wfl_class()
            if form.cleaned_data['var_name'] in wf.data_files_vars:
                #check 2: 'var_name' wasn't uploaded before
                uploaded_before = UploadedData.objects.filter(exp=exp, var_name=form.cleaned_data['var_name'])
                if len(uploaded_before) == 0:
                    ud = UploadedData(exp=exp, var_name=form.cleaned_data['var_name'])
                    ud.save()
                    ud.data = form.cleaned_data['data']
                    ud.save()
                else:
                    print "var_name %s was already uploaded " % (var_name, )
            else:
                print "var_name %s isn't used by exp %s " % (var_name, exp_id )

    return redirect(request.POST['next'])


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
    data_files = UploadedData.objects.filter(exp = exp)
    layout = exp.workflow
    wfl_class = dyn_import(layout.wfl_class)
    wf = wfl_class()

    if exp.status in ['done', 'failed']:
        template = loader.get_template(wf.template_result)
    else:
        template = loader.get_template(wf.template)

    r = get_redis_instance()
    key_context = "%s%s" % (CTX_STORE_REDIS_PREFIX, exp.e_id)
    pickled_ctx = r.get(key_context)
    if pickled_ctx is not None:
        ctx = pickle.loads(pickled_ctx)
    else:
        ctx = {"error": "context wasn't stored"}

    context = RequestContext(request, {
        "exp_page_active": True,

        "data_files": data_files,
        "data_files_var_names_uploaded": [df.var_name for df in data_files],
        "data_files_var_names_required": wf.data_files_vars,
        "show_uploads": len(data_files) != wf.data_files_vars,
        "exp": exp,
        "layout": layout,
        "ctx": ctx,
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

    exp = Experiment(
        author=request.user,
        workflow=layout,
    )
    exp.save()

    template = loader.get_template(wf.template)
    context = RequestContext(request, {
        #"next":"/add_experiment",
        "exp_add_page_active": True,

        "layout": layout,
        "wf": wf,
        "exp": exp,
    })

    return redirect("/experiment/%s" % exp.e_id) # TODO use reverse
    #return HttpResponse(template.render(context))


@login_required(login_url='auth/login/')
def create_exp_instance(request):
    #layout_id = int(request.POST['id_wfl'])
    exp_id = int(request.POST['exp_id'])
    exp = Experiment.objects.get(e_id=exp_id)
    exp.status = 'configured'
    exp.save()
    #layout = WorkflowLayout.objects.get(w_id=layout_id)
    layout = exp.workflow
    wfl_class = dyn_import(layout.wfl_class)
    wf = wfl_class()

    main_task, ctx = wf.get_workflow(request)

    #import ipdb; ipdb.set_trace()
    ctx['exp_id'] = exp.e_id
    exc_task.s(ctx, main_task, set_exp_status).apply_async()
    # on finish should update status

    return redirect("/experiments")


