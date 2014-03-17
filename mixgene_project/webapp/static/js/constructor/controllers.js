Constructor.controller('MainCtrl', function($scope, blockAccess){
    $scope.show_old_worktable = false;
    $scope.access = blockAccess;
    $scope.exp_id = $scope.access.exp_id;
    $scope.root_scope_name = "root";
    $scope.mode = {};
//    $scope.ro_mode = false;

    $scope.init = function(exp_id, ro_mode){

        if( typeof(ro_mode) == "undefined"){
            $scope.mode.ro = false;
        } else {
            $scope.mode.ro = ro_mode;
        }
//        alert(angular.toJson($scope.mode));
        $scope.access.init(exp_id, $scope.mode);
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

    $scope.bscope = $scope.block.sub_scope;
    $scope.toggleBodyVisibility = function(){
        $scope.block.ui_folded = !$scope.block.ui_folded;
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
    $scope.show_set_label_control = false;
    $scope.add_var = function(){
        var key = $scope.block.collector_spec.new.scope_var;
        $scope.block.collector_spec.new.data_type = $scope.access.vars_by_key[key].data_type;
        $scope.access.send_action($scope.block, "add_collector_var", true);
    }

    $scope.remove_from_collector = function(name){
        $scope.block.collector_spec["to_remove"] = name;
        $scope.access.send_action($scope.block, "remove_collector_var");
    }

    $scope.show_add_button = function(){
        return _.contains(
            _.map($scope.block.actions, function(obj){
                return obj.code
            }),
            'save_params'
        );
    }

})

Constructor.controller('DataFlowRenderCtrl', function($scope, $sce){
    $scope.access.exp_sub_resource("get_dataflow_graphviz",
        function(response){
//            $scope.viz_placeholder =  $sce.trustAsHtml(Viz(response.data, "svg"));
            $scope.viz_placeholder =  $sce.trustAsHtml(response.data);
            document._graph = response.data;
        }
    )
})

Constructor.controller('CustomIteratorAddCellFieldCtrl', function($scope){
    $scope.add_cell_field = function(){
        $scope.access.send_action($scope.block, "add_cell_prototype_field", false)
    }
})
Constructor.controller('CustomIteratorDynInputsCtrl', function($scope){

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

Constructor.controller('RcTableCtrl', function($scope, $sce){

    $scope.$watch('block.table_config.header_axis', function(newVal){
        if( $scope.block.table_config){
            $scope.block.table_config.multi_index_axis_dict[newVal] = "";
        }
    });

    $scope.$watch('block.table_config.multi_index_axis_dict', function(newVal){
        if( $scope.block.table_config){
            if( newVal[$scope.block.table_config.header_axis]){
                $scope.block.table_config.header_axis = "";
            }
        }
    }, true);

    $scope.$watch('block.table.html', function(newVal){
        if( newVal){
            $scope.safe_table = $sce.trustAsHtml(newVal);
        }
    });
})

Constructor.controller('BoxPlotCtrl', function($scope){
    $scope.redraw_plot = function(){
        $scope.access.send_action($scope.block, "save_params", false,
            function(){
                console.log("block should be updated");
                console.log($scope.block.chart_series);
                console.log($scope.access.block_bodies[$scope.block.uuid].chart_series);

                $scope.plotConfig.series = $scope.access.block_bodies[$scope.block.uuid].chart_series;
                $scope.plotConfig.options.xAxis.categories =
                    $scope.access.block_bodies[$scope.block.uuid].chart_categories;

            }
        );
    }

    $scope.plotConfig = {
        options: {
            chart: {
                type: 'boxplot',
                inverted: true
            },
            legend: false,

            xAxis: {
                categories: $scope.block.chart_categories,
                title: {
                    text: 'Inputs'
                },
                labels: {
                    rotation: 0,
                    align: 'right',
                    style: {
                        fontSize: '10px',
                        fontFamily: 'Verdana, sans-serif'
                    }
                }
            },
            yAxis: {
                title: {
                    text: 'Scores'
                }
            }

        },

        series: $scope.block.chart_series,
        title: {
            text: ''
        },
        credits: {
            enabled: false
        },
        loading: false
    }

})