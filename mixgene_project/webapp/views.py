import csv
import json
from collections import defaultdict

import numpy as np

from django.http import HttpResponse, HttpResponseNotAllowed, HttpRequest, HttpResponseBadRequest
from django.template import RequestContext, loader
from django.shortcuts import redirect

from django.views.decorators.debug import sensitive_post_parameters
from django.views.decorators.cache import never_cache
from django.views.decorators.csrf import csrf_protect

from django.contrib.auth import authenticate, login, logout
from django.contrib.auth.models import User
from django.contrib.auth.decorators import login_required

from webapp.models import Experiment, UploadedData, delete_exp
from webapp.forms import UploadForm
from webapp.scope import Scope
from webapp.store import add_block_to_exp_from_dict
from workflow.blocks import blocks_by_group

from mixgene.util import dyn_import, get_redis_instance, mkdir


def index(request):
    template = loader.get_template('index.html')
    context = RequestContext(request, {
        "next": "/",
    })
    return HttpResponse(template.render(context))


def about(request):
    template = loader.get_template('about.html')
    context = RequestContext(request, {
        "next": "/",
    })
    return HttpResponse(template.render(context))


def contact(request):
    template = loader.get_template('contact.html')
    context = RequestContext(request, {
        "next": "/",
    })
    return HttpResponse(template.render(context))


def constructor(request, exp_id):
    exp = Experiment.objects.get(pk=exp_id)

    context = {
        "next": "/",
        "scope": "root",
        "exp": exp,
        "exp_json": json.dumps({
            "exp_id": exp_id,
        }),
    }

    template = loader.get_template('constructor.html')
    context = RequestContext(request, context)
    return HttpResponse(template.render(context))


@csrf_protect
def blocks_resource(request, exp_id):
    allowed = ["GET", "POST"]
    if request.method not in allowed:
        return HttpResponseNotAllowed(["GET", "POST"])

    exp = Experiment.objects.get(pk=exp_id)
    r = get_redis_instance()

    if request.method == "POST":
        try:
            received_block = json.loads(request.body)
        except Exception, e:
            # print "BODY: X%sX " % request.body
            return HttpResponseBadRequest()
        add_block_to_exp_from_dict(exp, received_block)


    # TODO: Move to model logic
    blocks_uuids = exp.get_all_block_uuids(redis_instance=r)
    blocks = exp.get_blocks(blocks_uuids, redis_instance=r)

    block_bodies = {
        block.uuid: block.to_dict()
        for uuid, block in blocks
    }
    blocks_by_bscope = defaultdict(list)
    for uuid, block in blocks:
        blocks_by_bscope[block.scope_name].append(uuid)

    aliases_map = exp.get_block_aliases_map(redis_instance=r)

    root_blocks = [block.to_dict() for
                uuid, block in blocks if block.scope_name == "root"]

    scopes = {}

    for scope_name, _ in exp.get_all_scopes_with_block_uuids(redis_instance=r).iteritems():
        scope = Scope(exp, scope_name)
        scope.load(redis_instance=r)
        scope.update_scope_vars_by_block_aliases(aliases_map)

        scopes[scope_name] = scope.to_dict()

    result = {
        "blocks": root_blocks,

        "blocks_by_bscope": blocks_by_bscope,
        "block_bodies": block_bodies,

        "blocks_by_group": blocks_by_group,
        "scopes": scopes,

    }
    resp = HttpResponse(content_type="application/json")
    # import ipdb; ipdb.set_trace()
    json.dump(result, resp)
    return resp


@csrf_protect
def block_resource(request, exp_id, block_uuid, action_code=None):
    """

    @type request: HttpRequest
    """
    exp = Experiment.objects.get(pk=exp_id)
    # import ipdb; ipdb.set_trace()
    block = exp.get_block(str(block_uuid))

    import time; time.sleep( 0.05)
    if request.method == "POST":
        try:
            received_block = json.loads(request.body)
            print received_block
        except Exception, e:
            # TODO log errors
            received_block = {}
        block.apply_action_from_js(action_code, exp=exp, request=request, received_block=received_block)

    if request.method == "GET" or request.method == "POST":
        block_dict = exp.get_block(block_uuid).to_dict()
        resp = HttpResponse(content_type="application/json")
        json.dump(block_dict, resp)
        return resp

    return HttpResponseNotAllowed(["POST", "GET"])


#@csrf_protect
def block_field_resource(request, exp_id, block_uuid, field):
    exp = Experiment.objects.get(pk=exp_id)
    # import ipdb; ipdb.set_trace()
    block = exp.get_block(str(block_uuid))
    data = getattr(block, field)(exp, request)

    resp = HttpResponse(content_type="application/json")
    json.dump(data, resp)
    return resp


def block_sub_page(request, exp_id, block_uuid, sub_page):
    exp = Experiment.objects.get(pk=exp_id)
    block = exp.get_block(block_uuid)

    template = loader.get_template(block.pages[sub_page]['widget'])
    context = {
        "block_": block,
        "exp": exp,
    }
    context = RequestContext(request, context)
    return HttpResponse(template.render(context))

@csrf_protect
@never_cache
def upload_data(request):
    if request.method == "POST":
        form = UploadForm(request.POST, request.FILES)
        if form.is_valid():
            exp_id = form.cleaned_data['exp_id']
            block_uuid = form.cleaned_data['block_uuid']
            field_name = form.cleaned_data['field_name']
            file_obj = form.cleaned_data['file']

            exp = Experiment.get_exp_by_id(exp_id)
            block = exp.get_block(block_uuid)
            block.save_file_input(exp, field_name, file_obj, request.POST["upload_meta"])

    return HttpResponse(status=204)


@csrf_protect
@never_cache
def create_user(request):
    if 'username' not in request.POST:
        template = loader.get_template('auth/user_creation.html')
        context = RequestContext(request, {
            "next": "/",
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
            User.objects.create_user(username, email, password)
            user = authenticate(username=username, password=password)
            login(request, user)

        # add user created page, or auto
        return redirect("/")


@login_required(login_url='/auth/login/')
@never_cache
def experiments(request):
    template = loader.get_template('experiments.html')
    context = RequestContext(request, {
        "exps": Experiment.objects.filter(author=request.user),
        "next": "/experiments",
        "exp_page_active": True,
    })
    return HttpResponse(template.render(context))


@login_required(login_url='/auth/login/')
@csrf_protect
@never_cache
def alter_exp(request, exp_id, action):
    exp = Experiment.objects.get(pk=exp_id)
    if exp.author != request.user:
        return redirect("/") # TODO: show alert about wrong experiment

    if action == "execute":
        exp.execute()

    if action == 'delete': # TODO: check that exp state allows deletion
        delete_exp(exp)

    if action == 'update':
        exp.validate(request)

    # if action == 'save_gse_classes':
    #     factors = json.loads(request.POST['factors'])
    #     exp.update_ctx({"gse_factors": factors})

    return redirect(request.POST.get("next") or "/constructor/%s" % exp.pk) # TODO use reverse


@login_required(login_url='/auth/login/')
def add_experiment(request):
    exp = Experiment.objects.create(
        author=request.user,
        status='initiated',  # TODO: until layout configuration will be implemented
    )

    # TODO: move all init stuff to the model
    exp.save()
    exp.post_init()

    mkdir(exp.get_data_folder())
    return redirect("/constructor/%s" % exp.pk) # TODO use reverse

#@login_required(login_url='/auth/login/')
def get_flot_2d_scatter(request, exp_id, filename):
    exp = Experiment.objects.get(pk = exp_id)
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


#@cache_page(60 * 15)
def get_gse_samples_info(request, exp_id, block_uuid):
    exp = Experiment.objects.get(pk=exp_id)
    block = exp.get_block(block_uuid)
    #assert isinstance(block, FetchGSE)

    #TODO: make common interface to access block variables from JS client
    pheno_df = block.get_out_var("expression_set").get_pheno_data_frame()
    pheno_headers = [pheno_df.index.name]
    pheno_headers.extend(pheno_df.columns.tolist())

    classes = []
    if 'User_class' in pheno_df.columns:
        for _, x in tuple(pheno_df[['User_class']].to_records()):

            if isinstance(x, float) and np.isnan(x):
                pass
            else:
                classes.append(str(x))
    else:
        pheno_headers.append('User_class')
    classes = list(set(classes))

    pheno = []
    for rec in pheno_df.to_records():
        row = []
        for cell in tuple(rec):
            if isinstance(cell, float) and np.isnan(cell):
                row.append('')
            else:
                row.append(str(cell))
        pheno.append(row)
    #import ipdb; ipdb.set_trace()
    result = {
        "classes": classes,
        "pheno": pheno,
        "pheno_headers": pheno_headers,

    }
    resp = HttpResponse(content_type="application/json")
    json.dump(result, resp)

    #TODO: cache this
    return resp
