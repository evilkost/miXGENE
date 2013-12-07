var $exp_id = $("#exp_id_storage").attr("value");
var $csrf_token = $("#csrf_token_storage").attr("value");

var spin_opts = {
    lines: 9, // The number of lines to draw
    length: 0, // The length of each line
    width: 3, // The line thickness
    radius: 6, // The radius of the inner circle
    corners: 1, // Corner roundness (0..1)
    rotate: 0, // The rotation offset
    direction: 1, // 1: clockwise, -1: counterclockwise
    color: '#000', // #rgb or #rrggbb or array of colors
    speed: 1, // Rounds per second
    trail: 37, // Afterglow percentage
    shadow: true, // Whether to render a shadow
    hwaccel: false, // Whether to use hardware acceleration
    className: 'spinner', // The CSS class to assign to the spinner
    zIndex: 2e9, // The z-index (defaults to 2000000000)
    top: 'auto', // Top position relative to parent in px
    left: 'auto' // Left position relative to parent in px
};

function block_action_click(eventObj){
    document.obj = eventObj;
    var $target = $(eventObj.target);
    var block_uuid = $target.attr("data-block-uuid")
    var action_name = $target.attr("href").substr(1);

    var url = "/update_block/" + action_name;
    function onDataReceived(response){
        $("#"+block_uuid).replaceWith(response)
        restore_bindings_after_injection(block_uuid);
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
    restore_bindings_after_injection(block_uuid);
}

function add_spinner($obj){
    new Spinner(spin_opts).spin($obj)
}

function restore_bindings_after_injection(block_uuid){
    $("#"+block_uuid).find(".form-ajax-submit").ajaxForm(ajaxFormShowResponse);
    //$("#"+block_uuid).find(".btn-block-action").click(block_action_click);
    add_spinner($('#'+block_uuid+' .spin-placeholder'))
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
    var scope = $(event_obj.target).attr("data-scope");
    $("#blocks-pallet-"+scope).show();
    block_placeholder_handle = this;
    //alert($(event_obj.target).attr('class'));
    $(event_obj.target).hide()
});


$(".btn-block-provider").click(function(event_obj) {
    var block = $(event_obj.target).attr("href").substr(1);
    var scope = $(event_obj.target).attr("data-scope");
    var url = "/add_block";

    function onDataReceived(response){
        $(block_placeholder_handle).before(response);
        var block_uuid = $(response).attr('id');

        restore_bindings_after_injection(block_uuid);
        document.block_uuid = block_uuid;

        blocks_list.push(block_uuid);

        $("#blocks-pallet-"+scope).hide();
        $(".btn-block-placeholder").show();
    }

    $.ajax({
        url: url,
        type: "POST",
        dataType: "html",
        success: onDataReceived,
        data: {
            csrfmiddlewaretoken: $csrf_token,
            block: block,
            scope: scope,
            exp_id: $exp_id
        }
    });
});