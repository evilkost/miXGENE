# -*- coding: utf-8 -*-
from django import forms


class UploadForm(forms.Form):
    file = forms.FileField()
    exp_id = forms.IntegerField()
    block_uuid = forms.CharField()
    field_name = forms.CharField()
    multiple = forms.BooleanField(required=False)
