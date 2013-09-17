import cPickle as pickle
import csv
import json
from collections import defaultdict

from django.http import HttpResponse
from django.template import RequestContext, loader
from django.shortcuts import redirect

from django.views.decorators.debug import sensitive_post_parameters
from django.views.decorators.cache import never_cache
from django.views.decorators.csrf import csrf_protect

from django.contrib.auth import authenticate, login, logout
from django.contrib.auth.models import User
from django.contrib.auth.decorators import login_required

from webapp.models import Experiment, WorkflowLayout, UploadedData, delete_exp
from webapp.forms import UploadForm

from workflow.actions import exc_action, set_exp_status
from workflow.layout import write_result
from workflow.common_tasks import fetch_GEO_gse_matrix

from mixgene.util import dyn_import, get_redis_instance, mkdir
from mixgene.redis_helper import ExpKeys


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

            wf = exp.workflow.get_class_instance()
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
def geo_fetch_data(request):
    if request.method == "POST":
        print "invoked"
        exp_id = int(request.POST['exp_id'])
        dataset_id = request.POST['dataset_id']
        dataset_var = request.POST['dataset_var']

        exp = Experiment.objects.get(e_id=exp_id)

        ctx = exp.get_ctx()
        ctx["exp_fetching_data_var"].add(dataset_var)
        exp.update_ctx(ctx)

        #TODO: check "GSE" prefix
        uid = dataset_id[3:]
        dir_path = exp.get_data_folder()
        st = fetch_GEO_gse_matrix.s(exp, dataset_var, uid, dir_path)
        st.apply_async()
        return redirect(request.POST['next'])

    else:
        return redirect("/")

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

#@login_required(login_url='/auth/login/')

def exp_details(request, exp_id):
    exp = Experiment.objects.get(e_id = exp_id)
    data_files = UploadedData.objects.filter(exp = exp)
    layout = exp.workflow
    wf = layout.get_class_instance()

    if exp.status in ['done', 'failed']:
        template = loader.get_template(wf.template_result)
    else:
        template = loader.get_template(wf.template)

    ctx = exp.get_ctx()
    context = RequestContext(request, {
        "exp_page_active": True,

        "data_files": data_files,
        "data_files_var_names_uploaded": [df.var_name for df in data_files],
        "data_files_var_names_required": wf.data_files_vars,
        "show_uploads": len(data_files) != wf.data_files_vars,

        "data_files_url_prefix": "/media/data/%s/%s/" % (exp.author.id, exp.e_id),
        "exp": exp,
        "layout": layout,
        "ctx": ctx,
        "next": "/experiment/%s" % exp.e_id,
        "runnable": exp.status == "configured",
        "configurable": exp.status in ["initiated", "configured"],
    })
    return HttpResponse(template.render(context))


@login_required(login_url='/auth/login/')
def alter_exp(request, exp_id, action):
    exp = Experiment.objects.get(e_id = exp_id)
    if exp.author != request.user:
        return redirect("/") # TODO: show alert about wrong experiment

    if action == 'delete': # TODO: check that exp state allows deletion
        delete_exp(exp)

    wf = exp.workflow.get_class_instance()
    if action == 'run':
        # check status & if all right run experiment
        wf.run_experiment(exp)

    if action == 'update':
        new_ctx, errors = wf.validate_exp(exp, request)
        if errors is None:
            exp.status = "configured"
        else:
            exp.status = "initiated"
        exp.update_ctx(new_ctx)
        exp.save()

    return redirect(request.POST.get("next") or "/experiment/%s" % exp.e_id) # TODO use reverse


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
def create_experiment(request, layout_id):
    layout = WorkflowLayout.objects.get(w_id=layout_id)
    wfl_class = dyn_import(layout.wfl_class)
    wf = wfl_class()

    exp = Experiment(
        author=request.user,
        workflow=layout,
        status='initiated', # TODO: until layout configuration will be implemented
    )
    exp.save()
    exp.update_ctx({
        "exp_id": exp.e_id,
        "exp_fetching_data_var": set(),
    })

    mkdir(exp.get_data_folder())

    template = loader.get_template(wf.template)
    context = RequestContext(request, {
        "exp_add_page_active": True,
        "layout": layout,
        "wf": wf,
        "exp": exp,
    })
    return redirect("/experiment/%s" % exp.e_id) # TODO use reverse

#@login_required(login_url='/auth/login/')
def get_flot_2d_scatter(request, exp_id, filename):
    exp = Experiment.objects.get(e_id = exp_id)
    filepath = exp.get_data_file_path(filename)

    points_by_class = defaultdict(list)
    with open(filepath) as inp:
        cr = csv.reader(inp, delimiter=' ', quotechar='"')
        axes_names = cr.next()
        for cls, x1, x2 in cr:
            points_by_class[cls].append([float(x1), float(x2)])

    cls_set = points_by_class.keys()

    series_list = []
    for cls in cls_set:
        series_list.append({
            "label": cls,
            "data": points_by_class[cls]
        })
    result = {
        "series_list": series_list,
        "x_axis_name": axes_names[0],
        "y_axis_name": axes_names[1],
    }
    
    resp = HttpResponse(content_type="application/json")
    json.dump(result, resp)
    return resp
    """ json
    {
        "label": "Europe (EU27)",
        "data": [[1999, 3.0], [2000, 3.9], [2001, 2.0], [2002, 1.2], [2003, 1.3], [2004, 2.5], [2005, 2.0], [2006, 3.1], [2007, 2.9], [2008, 0.9]]
    }
    """




