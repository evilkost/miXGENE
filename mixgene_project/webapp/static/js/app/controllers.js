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

    $scope.show_panel_body = true;

    $scope.bscope = $scope.block.sub_scope;
    $scope.toggleBodyVisibility = function(){
        $scope.show_panel_body = !$scope.show_panel_body;
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
})


Constructor.controller('formFieldCtrl', function($scope, $log){
    $scope.template = "/static/js/app/forms/field_" +
        $scope.$parent.param_proto.input_type + ".html";

    $scope.field = $scope.param_proto;
})

Constructor.controller('blockElementCtrl', function($scope){
    $scope.template = "/static/js/app/elements/" + $scope.element;

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