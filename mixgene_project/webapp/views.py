import cPickle as pickle
import csv
import json
import gzip
from pprint import pprint
from collections import defaultdict

import numpy as np

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
from webapp.store import add_block_to_exp_from_request
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
    exp = Experiment.objects.get(e_id=exp_id)
    r = get_redis_instance()

    ctx = exp.get_ctx()
    blocks_uuids = exp.get_all_block_uuids(redis_instance=r)
    # TODO: remove blocks from direct rendering
    # all of them should be loaded by ajax
    #  than block wouldnt contain temporary data

    blocks = [(block_uuid, exp.get_block(block_uuid)) for
              block_uuid in blocks_uuids]

    blocks_jsonified = dict([(block_uuid, exp.get_block(block_uuid).serialize_to_dict(exp)) for
                       block_uuid in blocks_uuids])
    context = {
        "next": "/",
        "scope": "root",
        "exp": exp,
        "ctx": ctx,
        "blocks": blocks,
        "blocks_jsonified": json.dumps(blocks_jsonified),
        "blocks_by_group": blocks_by_group,  # TODO: NAMES <- block which can be added
        "blocks_by_group_json": json.dumps(blocks_by_group),  # TODO: NAMES <- block which can be added
        #"blocks_by_provided_data_type":
        #    exp.group_blocks_by_provided_type(redis_instance=r),
    }
    for _, block in blocks:
        context.update(block.before_render(exp))

    template = loader.get_template('constructor.html')
    #pprint(context)
    context = RequestContext(request, context)
    return HttpResponse(template.render(context))


def _render_block(request, exp, block):
    block.before_render(exp)
    template = loader.get_template(block.widget)
    context = {
        "exp_block": block,
        "exp": exp,
        "ctx": exp.get_ctx(),
    }
    context = RequestContext(request, context)
    return HttpResponse(template.render(context))


@csrf_protect
def add_block(request):
    exp = Experiment.get_exp_from_request(request)
    block = add_block_to_exp_from_request(request)
    return _render_block(request, exp, block)


def render_block(request):
    exp = Experiment.get_exp_from_request(request)
    block = exp.get_block(request.POST["block_uuid"])
    return _render_block(request, exp, block)


@csrf_protect
def update_block(request):
    if request.method == "POST":
        exp = Experiment.get_exp_from_request(request)
        #ctx = exp.get_ctx()
        #import ipdb; ipdb.set_trace()
        action = request.POST['action']
        block = exp.get_block(request.POST["block_uuid"])
        block.do_action(action, exp=exp, request=request)
        return _render_block(request, exp, block)
    return HttpResponse("")


def block_sub_page(request, exp_id, block_uuid, sub_page):
    exp = Experiment.objects.get(e_id=exp_id)
    ctx = exp.get_ctx()
    block = exp.get_block(block_uuid)
    print block.uuid

    template = loader.get_template(block.pages[sub_page])
    context = {
        "block_": block,
        "ctx": ctx,
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
            exp = Experiment.objects.get(e_id=exp_id)
            ctx = exp.get_ctx()
            var_name = form.cleaned_data['var_name']
            inp_var = ctx["input_vars"][var_name]
            if inp_var is None:
                print "var_name %s isn't used by exp %s " % (var_name, exp_id )
                #TODO: hmm shouldn't actually happen
            else:
                uploaded_before = UploadedData.objects.filter(exp=exp, var_name=var_name)
                if len(uploaded_before) == 0:
                    ud = UploadedData(exp=exp, var_name=var_name)
                    ud.data = form.cleaned_data['data']
                    ud.save()

                    inp_var.is_done = True
                    inp_var.set_file_type("user")
                    inp_var.filename = ud.data.name.split("/")[-1]
                    exp.update_ctx(ctx)
                else:
                    print "var_name %s was already uploaded " % (var_name, )
    return redirect(request.POST['next'])

"""
@csrf_protect
@never_cache
def geo_fetch_data(request):
    if request.method == "POST":
        exp_id = int(request.POST['exp_id'])
        exp = Experiment.objects.get(e_id=exp_id)

        var_name = request.POST['var_name']
        geo_uid = request.POST['geo_uid']
        file_format = request.POST['file_format']

        ctx = exp.get_ctx()
        ctx["input_vars"][var_name].is_being_fetched = True
        exp.update_ctx(ctx)

        #TODO: check "GSE" prefix

        #st = fetch_geo_gse.s(exp, var_name, geo_uid, file_format)
        #st.apply_async()
        return redirect(request.POST['next'])

    else:
        return redirect("/")
"""

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

#@login_required(login_url='/auth/login/')

@never_cache
def exp_details(request, exp_id):
    exp = Experiment.objects.get(e_id = exp_id)
    layout = exp.workflow
    wf = layout.get_class_instance()

    if exp.status in ['done', 'failed']:
        template = loader.get_template(wf.template_result)
    else:
        template = loader.get_template(wf.template)

    ctx = exp.get_ctx()
    if 'results' not in ctx.keys():
        ctx['results'] = {}
    for res_var in ctx['result_vars']:
        if res_var in ctx:
            ctx['results'][res_var] = ctx[res_var]

    context = RequestContext(request, {
        "exp_page_active": True,

        "data_files_url_prefix": "/media/data/%s/%s/" % (exp.author.id, exp.e_id),
        "exp": exp,
        "layout": layout,
        "wf": wf,
        "ctx": ctx,
        "next": "/experiment/%s" % exp.e_id,
        "runnable": exp.status == "configured",
        "configurable": exp.status in ["initiated", "configured"],
    })
    return HttpResponse(template.render(context))


@login_required(login_url='/auth/login/')
@csrf_protect
@never_cache
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
        exp.validate(request)

    if action == 'save_gse_classes':
        factors = json.loads(request.POST['factors'])
        exp.update_ctx({"gse_factors": factors})
        print factors

    return redirect(request.POST.get("next") or "/experiment/%s" % exp.e_id) # TODO use reverse


@login_required(login_url='/auth/login/')
def add_experiment(request):
    #TODO: remove this temporary solution
    layout = WorkflowLayout.objects.get(wfl_class="workflow.layout.DummyWfl")
    wfl_class = dyn_import(layout.wfl_class)
    wf = wfl_class()

    exp = Experiment(
        author=request.user,
        workflow=layout,
        status='initiated',  # TODO: until layout configuration will be implemented
    )
    exp.save()
    ctx = wf.init_ctx
    ctx.update({
        "exp_id": exp.e_id,

        "input_vars": wf.input_vars,
        "result_vars": wf.result_vars,
    })
    exp.init_ctx(ctx)

    mkdir(exp.get_data_folder())

    return redirect("/constructor/%s" % exp.e_id) # TODO use reverse



# @login_required(login_url='/auth/login/')
# def add_experiment(request):
#     template = loader.get_template('add_experiment.html')
#     context = RequestContext(request, {
#         "next": "/add_experiment",
#         "exp_add_page_active": True,
#         "all_layouts": WorkflowLayout.objects.all()
#     })
#     return HttpResponse(template.render(context))
#
# @login_required(login_url='auth/login/')
# def create_experiment(request, layout_id):
#     layout = WorkflowLayout.objects.get(w_id=layout_id)
#     wfl_class = dyn_import(layout.wfl_class)
#     wf = wfl_class()
#
#     exp = Experiment(
#         author=request.user,
#         workflow=layout,
#         status='initiated',  # TODO: until layout configuration will be implemented
#     )
#     exp.save()
#     ctx = wf.init_ctx
#     ctx.update({
#         "exp_id": exp.e_id,
#
#         "input_vars": wf.input_vars,
#         "result_vars": wf.result_vars,
#     })
#     exp.init_ctx(ctx)
#
#     mkdir(exp.get_data_folder())
#
#     template = loader.get_template(wf.template)
#     context = RequestContext(request, {
#         "exp_add_page_active": True,
#         "layout": layout,
#         "wf": wf,
#         "exp": exp,
#     })
#     return redirect("/experiment/%s" % exp.e_id) # TODO use reverse

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


#@cache_page(60 * 15)
def get_gse_samples_info(request, exp_id, block_uuid):
    exp = Experiment.objects.get(e_id=exp_id)
    block = exp.get_block(block_uuid)
    #assert isinstance(block, FetchGSE)

    pheno_df = block.expression_set.get_pheno_data_frame()
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


def get_csv_as_table(request, exp_id, filename):
    exp = Experiment.objects.get(e_id = exp_id)
    ctx = exp.get_ctx()
    filepath = exp.get_data_file_path(filename)
    template = loader.get_template('elements/table.html')

    has_row_names = bool(request.POST.get('has_row_names', False)) # if has_row_names and has_col_names
    has_col_names = bool(request.POST.get('has_col_names', False)) # than first column has no value!
    row_names_header = request.POST.get('row_names_header', '')

    get_header_from_ctx = bool(request.POST.get('get_header_from_ctx', False))
    ctx_header_key = request.POST.get('ctx_header_key')

    csv_delimiter = str(request.POST.get('delimiter', ' '))
    csv_quotechar = str(request.POST.get('quotechar', '"'))

    rows_num_limit = int(request.POST.get('rows_num_limit', 100))

    rows = []
    header = None
    with open(filepath) as inp:
        cr = csv.reader(inp, delimiter=csv_delimiter, quotechar=csv_quotechar)
        if has_col_names:
            header = cr.next()
            if has_row_names:
                header.insert(0, row_names_header)
        if get_header_from_ctx:
            header = ctx[ctx_header_key]
        row_num = 0
        while row_num <= rows_num_limit:
            try:
                rows.append(cr.next())
            except:
                break
            row_num += 1

    print header
    context = RequestContext(request, {
        "rows": rows,
        "header": header,
    })

    return HttpResponse(template.render(context))
