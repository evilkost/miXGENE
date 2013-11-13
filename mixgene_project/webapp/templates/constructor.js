var $exp_id = $("#exp_id_storage").attr("value");
var $csrf_token = $("#csrf_token_storage").attr("value");

function block_action_click(eventObj){
    document.obj = eventObj;
    var $target = $(eventObj.target);
    var block_uuid = $target.attr("data-block-uuid")
    var action_name = $target.attr("href").substr(1);

    var url = "/update_widget/" + action_name;
    function onDataReceived(response){
        $("#"+block_uuid).replaceWith(response)
        register_block_callbacks(block_uuid);
    }
    $.ajax({
        url: url,
        type: "POST",
        dataType: "html",
        success: onDataReceived,
        data: {
            csrfmiddlewaretoken: $csrf_token,
            block_uuid: block_uuid,
            exp_id: $exp_id
        }
    });
}

function ajaxFormShowResponse(response_text, status_text, xhr, $form){
    var block_uuid = $form.children("input[name='block_uuid']").attr('value');
    $("#"+block_uuid).replaceWith(response_text);
    register_block_callbacks(block_uuid);
}

function register_block_callbacks(block_uuid){
    $("#"+block_uuid).find(".form-ajax-submit").ajaxForm(ajaxFormShowResponse);
    //$("#"+block_uuid).find(".btn-block-action").click(block_action_click);
}


$(document).ready(function (){
    $(".form-ajax-submit").ajaxForm(ajaxFormShowResponse);
    $(".btn-block-action").click(block_action_click);
});

// get from server stored configuration
var blocks_list = [];
var block_config = {};

var block_placeholder_handle = null;

$(".btn-block-placeholder").click(function(event_obj) {
    $("#blocks-pallet").show();
    block_placeholder_handle = this;
    //alert($(event_obj.target).attr('class'));
    $(event_obj.target).hide()
});


$(".btn-block-provider").click(function(event_obj) {
    var block = $(event_obj.target).attr("href").substr(1);
    var url = "/add_widget";

    function onDataReceived(response){
        $(block_placeholder_handle).before(response);
        var block_uuid = $(response).attr('id');

        register_block_callbacks(block_uuid);
        document.block_uuid = block_uuid;

        blocks_list.push(block_uuid);

        $("#blocks-pallet").hide();
        $(".btn-block-provider").show();
    }

    $.ajax({
        url: url,
        type: "POST",
        dataType: "html",
        success: onDataReceived,
        data: {
            csrfmiddlewaretoken: $csrf_token,
            block: block,
            exp_id: $exp_id
        }
    });
});