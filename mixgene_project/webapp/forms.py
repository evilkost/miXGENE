# -*- coding: utf-8 -*-
from django import forms

class UploadForm(forms.Form):
    data = forms.FileField(
        label='Select a file',
        help_text='max. 42 megabytes',
        required=False,
    )
    exp_id = forms.IntegerField()
    var_name = forms.CharField()
