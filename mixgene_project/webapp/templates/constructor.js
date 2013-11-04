var ajax_form_options = {
    beforeSubmit:  showRequest,  // pre-submit callback
    success:       showResponse  // post-submit callback
};

function showRequest(form_data, jq_form, options){
    /*
        document.form_data = form_data;
        document.jq_form = jq_form;
        document.option = options;
    */
    return true;
}
function showResponse(response_text, status_text, xhr, $form){
    block_uuid = $form.children("input[name='block_uuid']").attr('value');
    $("#"+block_uuid).replaceWith(response_text);
    $("#"+block_uuid).find(".form-ajax-submit").ajaxForm(ajax_form_options);

        document.response_text = response_text
        document.status_text = status_text
        document.xhr = xhr
        document._form = $form
    /* */
}

$(document).ready(function (){
    $(".form-ajax-submit").ajaxForm(ajax_form_options);
});

// get from server stored configuration
var blocks_list = [];
var block_config = {};

var plugin_placeholder_handle = null;

$(".plugin-placeholder").click(function(event_obj) {
    $("#plugins-pallet").show();
    plugin_placeholder_handle = this;
});


$(".btn-plugin-provider").click(function(event_obj) {
    plugin = $(event_obj.target).attr("href");
    url = "/add_widget";

    function onDataReceived(response){
        $(plugin_placeholder_handle).before(response);
        block_uuid = $(response).attr('id');
        $("#"+block_uuid).find(".form-ajax-submit").ajaxForm(ajax_form_options);
        document.block_uuid = block_uuid;

        blocks_list.push(block_uuid);

        $("#plugins-pallet").hide();
    }

    $.ajax({
        url: url,
        type: "POST",
        dataType: "html",
        success: onDataReceived,
        data: {
            csrfmiddlewaretoken: csrf_token,
            plugin: plugin,
            exp_id: $("#exp_id_storage").attr("value"),
        }
    });

});