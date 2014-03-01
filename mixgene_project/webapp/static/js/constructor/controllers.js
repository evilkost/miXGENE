Constructor.controller('MainCtrl', function($scope, blockAccess){
    $scope.show_old_worktable = false;
    $scope.access = blockAccess;
    $scope.exp_id = $scope.access.exp_id;
    $scope.root_scope_name = "root";

    $scope.toggle_show_old_worktable = function(){
        $scope.show_old_worktable = !$scope.show_old_worktable;
    }


})

Constructor.controller('WorktableCtrl', function WorktableCtrl($scope, blockAccess){
    $scope.access = blockAccess;
    $scope.bscope = "root";  // change for sub blocks worktable
    $scope.show_pallet = false;


})

Constructor.controller('BlockCtrl', function BlockCtrl($scope, blockAccess){
    $scope.access = blockAccess;
    $scope.exp_id = $scope.access.exp_id;
    $scope.has_errors = $scope.block.errors.length > 0;

//    $scope.show_panel_body = true;

    $scope.bscope = $scope.block.sub_scope;
    $scope.toggleBodyVisibility = function(){
//        $scope.show_panel_body = !$scope.show_panel_body;
        console.log("before " + $scope.block.ui_folded);
        $scope.block.ui_folded = !$scope.block.ui_folded;
        console.log("after " + $scope.block.ui_folded);
        $scope.access.send_action($scope.block, "toggle_ui_folded");
    };

    $scope.edit_base_name = false;
    $scope.toggleEditBaseName = function(){
        $scope.edit_base_name =!$scope.edit_base_name;

    }
    $scope.changeBaseName = function(){
        $scope.access.send_action($scope.block, "change_base_name", true);
        $scope.edit_base_name = false;
    }

})

Constructor.controller('PortGroupCtrl', function PortGroupCtrl($scope){
    //$scope.showPortGroup = $scope.ports != null;
    $scope.showPortGroup = true;

})

Constructor.controller('PortCtrl', function PortCtrl($scope, blockAccess){
    $scope.access = blockAccess;
    $scope.new_port = {name: ""};
    $scope.get_option_key = function( option ){
        return option.block_uuid + ":" + option.var_name;
    }
    $scope.get_option_title = function( option ){
        if( option != undefined && option != null){
            return option.block_alias + " -> " + option.var_name;
        } else {
            return "--error--"
        }
    }
    $scope.options = blockAccess.scopes[$scope.block.scope_name]
        .by_data_type[$scope.input.required_data_type];

    $scope.add_port = function(){
        // used only for dynamic input ports
        var block_to_send = $scope.block
        block_to_send._add_dyn_port = {
            "input": $scope.input.name,
            "new_port": $scope.new_port.name
        };
        $scope.new_port.name="";
        $scope.access.send_action(block_to_send, 'add_dyn_input', true);

    }
})


Constructor.controller('formFieldCtrl', function($scope, $log){
    $scope.template = "/static/js/constructor/forms/field_" +
        $scope.$parent.param_proto.input_type + ".html";

    $scope.predicate = "order_num";

    $scope.field = $scope.param_proto;
})

Constructor.controller('blockElementCtrl', function($scope){
    $scope.template = "/static/js/constructor/elements/" + $scope.element;

})
Constructor.controller('PalletCtrl', function($scope, blockAccess){
    $scope.show_pallet = false;
    $scope.access = blockAccess;
    $scope.toggle_pallet = function(){
        $scope.show_pallet = !$scope.show_pallet;
    }
    $scope.add_block = function(scopeName, block_name){
        $scope.access.add_block(scopeName, block_name);
        $scope.toggle_pallet();
    }
})

Constructor.controller('CollectorCtrl', function($scope, blockAccess){
    //$scope.access = blockAccess;
//    $scope.new_collector = {
//        "name": "",
//        "var": undefined
//    }

    $scope.add_var = function(){
        $scope.access.send_action($scope.block, "add_collector_var", true);
    }

})
Constructor.controller('LiCollectorOutputsCtrl', function($scope, blockAccess){
    $scope.bound_var_fixed =
        blockAccess.scopes[$scope.block.sub_scope_name].by_var_key[$scope.bound_var.pk];
})

Constructor.controller('UploadFieldCtrl', function($scope, $upload){
    $scope.upload_meta = {
        "exp_id": $scope.access.exp_id,
        "block_uuid": $scope.block.uuid,
        "field_name": $scope.field.name,
        "upload_meta": {}
    }
    $scope.stored_file = $scope.block[$scope.field.name];

    $scope.onFileSelect = function($files){
        for (var i = 0; i < $files.length; i++) {
            var file = $files[i];
            $scope.upload = $upload.upload({
                url: '/upload_data/', //upload.php script, node.js route, or servlet url
                // method: POST or PUT,
                // headers: {'headerKey': 'headerValue'},
                // withCredentials: true,
                data: $scope.upload_meta,
                file: file
                // file: $files, //upload multiple files, this feature only works in HTML5 FromData browsers
                /* set file formData name for 'Content-Desposition' header. Default: 'file' */
                //fileFormDataName: myFile, //OR for HTML5 multiple upload only a list: ['name1', 'name2', ...]
                /* customize how data is added to formData. See #40#issuecomment-28612000 for example */
                //formDataAppender: function(formData, key, val){} //#40#issuecomment-28612000
            }).progress(function(evt) {
                console.log('percent: ' + parseInt(100.0 * evt.loaded / evt.total));
            }).success(function(data, status, headers, config) {
                // file is uploaded successfully
                console.log(data);
            });
            //.error(...)
            //.then(success, error, progress);
        }
    }
})