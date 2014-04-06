'use strict';

var PhenotypeEditor = angular.module("PhenotypeEditor",
    ['ngCookies', 'ngTable', 'angularSpinner', 'ui.bootstrap']
    ,function ($interpolateProvider) {
        $interpolateProvider.startSymbol("{$");
        $interpolateProvider.endSymbol("$}");
    }
);

PhenotypeEditor.run(function ($http, $cookies) {
    $http.defaults.headers.common['X-CSRFToken'] = $cookies['csrftoken'];
});

PhenotypeEditor.factory("phenoIO", function($http){
    var io = {
        pheno: {},
        exp_id: undefined,
        block_uuid: undefined
    };

    io.init_source = function(exp_id, block_uuid){
        io.exp_id = exp_id;
        io.block_uuid = block_uuid;
    };

    io.fetch_data = function(cb){
        if( io.exp_id === undefined || io.block_uuid === undefined){
            alert("Fatal error, no identifiers to fetch data from server");

        };
        $http({
            method: "GET",
            url: "/experiments/" + io.exp_id + "/blocks/" + io.block_uuid + "/phenotype_for_js"
        }).success(function(data, status, headers, config){
            io.pheno = data;
            document._io = io;
            cb()
        })
    };

    io.send_classes = function(to_send, on_success){
        console.log("SENDING: " + angular.toJson(to_send));
        $http({
            method: "POST",
            url: "/experiments/" + io.exp_id+ "/blocks/"+
                io.block_uuid +"/actions/update_user_classes_assignment",
            data: angular.toJson(to_send)
        }).success(function(){
            if( on_success != undefined){
                on_success();
            }
        })
    }

    return io;
});

var ModalInstanceColumnVisibilitySelect = function($scope, $modalInstance, columns) {
    $scope.local_columns = columns;

    $scope.ok = function () {
        $modalInstance.close($scope.local_columns);
    };

    $scope.cancel = function () {
        $modalInstance.dismiss('cancel');
    };
};

PhenotypeEditor.controller('PhenoCtrl', function($scope, $modal, $log, phenoIO, $filter, ngTableParams){
    // JS magic to prevent text selection during SHIFT-CLICK
    window.onload = function() {
        document.onselectstart = function() {
            return false;
        }
    };

    $scope.init_done = false;
    $scope.phenoIO = phenoIO;

    $scope.available_classes = [];
    $scope.active_class = null;
    $scope.new_class_label = null;
    $scope.fnFilterUserClass = function(header){
        return header.field !== $scope.phenoIO.pheno.user_class_title;
    };

    $scope.new_column_name = "";
    $scope.show_add_column_form = false;
    $scope.toggle_add_column_form = function(){
        $scope.show_add_column_form = !$scope.show_add_column_form;
    };
    $scope.add_column = function(){
        $log.debug("adding new column: " + $scope.new_column_name);
        var dummy_classes = [];
        _.forEach($scope.phenoIO.pheno.table, function(value, key){
            dummy_classes.push(null);
        });
        if($scope.new_column_name == ""){
            toastr.warn("Can't add new target class column with an empty title")
        } else {
            var to_send = {
                "user_class_title": $scope.new_column_name,
                "classes": dummy_classes
            };
            $scope.phenoIO.send_classes(to_send, function(){
                window.location.reload();
            });
        }
    };

    var table_config = {};
    $scope.table_config = table_config;
    $scope.table_config["last_selected"] = 0;
    table_config["filter_dict"] = {};
    table_config["data"] = [];
    table_config["columns"] = [];
    table_config["tableParams"] = null;

    $scope.table_config.tableParams = new ngTableParams({
        page: 1,            // show first page
        count: 25          // count per page
        //        ,debugMode: false
        }, {
        total: $scope.table_config.data.length, // length of data
        getData: function($defer, params) {
            var filteredData;
            if(!$scope.init_done){
                var orderedData=[];
            } else {
                if($scope.table_config.filter_dict){
                    var fixed_filter_dict = {};
                    _.each($scope.table_config.filter_dict, function(value, key){
                        if(value != ""){
                            fixed_filter_dict[key] = value;
                        }
                    });
                    filteredData = $filter('filter')($scope.table_config.data, fixed_filter_dict);
                } else {
                    filteredData = $scope.table_config.data;

                }


                // use build-in angular filter
                var orderedData = params.sorting() ?
                    $filter('orderBy')(filteredData, params.orderBy()) :
                    filteredData;
            }

            params.total(orderedData.length); // set total for recalc pagination
            $defer.resolve(orderedData.slice((params.page() - 1) * params.count(), params.page() * params.count()));
            $scope.table_config.last_selected = 0;
        }
    });
    $scope.toggle_sorting = function(column){
        console.log(column);
        $scope.table_config.tableParams.sorting(
            column.field,
            $scope.table_config.tableParams.isSortBy(column.field, 'asc') ? 'desc' : 'asc'
        );
    };
    $scope.on_data_fetched_ng = function(){
        console.log($scope.phenoIO.pheno.headers);
        _.each($scope.phenoIO.pheno.headers, function(header){
            $scope.table_config.filter_dict[header.field]  = "";
            $scope.table_config.columns.push({
                "title": header.displayName,
                "field": String(header.field),
                "visible": header.visible
            });
        });
        $scope.table_config.data = $scope.phenoIO.pheno.table;
        $scope.update_available_classes()
    };
    $scope.clean_row_selection = function(){
        _.each($scope.table_config.tableParams.data, function(row){
            row.$selected = false;
        });
        $scope.table_config.last_selected = 0;
    };
    $scope.$watch("table_config.filter_dict", function () {
        if($scope.init_done && $scope.table_config && $scope.table_config.tableParams){
            $scope.table_config.tableParams.reload();
        }
    }, true);
    $scope.update_available_classes = function(){
        var classes = [""];
        var title_field = $scope.phenoIO.pheno.headers_title_to_code_map[
            $scope.phenoIO.pheno.user_class_title];
        angular.forEach($scope.phenoIO.pheno.table, function(value, key){
            if( value[title_field] != null && value[title_field] != undefined){
                classes.push(value[title_field].toString());
            }
        });

        $scope.available_classes = _.unique(classes);
//        console.log($scope.available_classes);
    };
    $scope.activate_class_for_assignment = function(class_title){
        console.log("activated: " + class_title);
        $scope.active_class = class_title;
    };
    $scope.add_new_class = function(){
        var new_class = $scope.new_class_label;
        if( new_class != null && new_class != undefined && new_class != ""){
            if(! _.contains($scope.available_classes, new_class)){
                $scope.available_classes.push(new_class);
                $scope.active_class = new_class;
            }
        };
        $scope.new_class_label = null;
    };
    $scope.assign_class = function(){
        var dst_title_field = $scope.phenoIO.pheno.headers_title_to_code_map[
            $scope.phenoIO.pheno.user_class_title];

        console.log("Assigning classes");
        console.log($scope.active_class);

        _.each($scope.table_config.tableParams.data, function(row){
            console.log(angular.toJson(row) + " is selected: " + row.$selected);
            if(row.$selected) {
                row[dst_title_field] = $scope.active_class;
            }
        });
        $scope.clean_row_selection();
    };
    $scope.on_data_fetched = function(){
        $scope.update_available_classes();
        $scope.on_data_fetched_ng();
        $scope.init_done = true;
    };
    $scope.init = function(exp_id, block_uuid){
        console.log("Initiated phenoIO with exp_id=" + exp_id + " block_uuid=" + block_uuid);
        $scope.phenoIO.init_source(exp_id, block_uuid);
        $scope.phenoIO.fetch_data($scope.on_data_fetched);
    };
    $scope.clone_feature_as_target_class = function(header){
        document._header = header;
        var src_title_field = header.field;
        var dst_title_field = $scope.phenoIO.pheno.headers_title_to_code_map[
            $scope.phenoIO.pheno.user_class_title];

        angular.forEach($scope.phenoIO.pheno.table, function(value, key){
            value[dst_title_field] = value[src_title_field];
        });

        $scope.update_available_classes();
        $scope.clean_row_selection();
    };
    $scope.save_assignment = function(){
        var classes = []
        var src_title_field = $scope.phenoIO.pheno.headers_title_to_code_map[
            $scope.phenoIO.pheno.user_class_title];
        angular.forEach($scope.phenoIO.pheno.table, function(value, key){
            classes.push(value[src_title_field]);
        });
        var to_send = {
            "user_class_title": $scope.phenoIO.pheno.user_class_title,
            "classes": classes
        };

        $scope.phenoIO.send_classes(to_send, function(){
            toastr.info("Class assignment was stored ");
        });
    };
    $scope.changeSelection = function(row, data, idx, event) {
        if( event.shiftKey){
            document.getSelection().removeAllRanges();
            var a = $scope.table_config.last_selected;
            var b = idx;
            if( a > b){
                var x = b; b = a; a = x;
            }
            for(var i=a; i<=b; i++){
                $scope.table_config.tableParams.data[i].$selected = true;
            }
        } else {
            $scope.table_config.tableParams.data[idx].$selected =
                !$scope.table_config.tableParams.data[idx].$selected;
        };
        $scope.table_config.last_selected = idx;
    };

    $scope.open_modal = function () {
        console.log("Open ");
        var modalInstance = $modal.open({
            templateUrl: '/static/js/pheno_editor/partials/visible_columns_modal.html',
            controller: ModalInstanceColumnVisibilitySelect,
            resolve: {
                columns: function () {
                    return $scope.table_config["columns"];
//                    return $scope.table_config.tableParams.columns;
                }
            }
        });

        modalInstance.result.then(function (foo) {
//            $log.info('1Modal dismissed at: ' + new Date() +' foo ' + foo);
        }, function () {
//            $log.info('2Modal dismissed at: ' + new Date());
        });
    };
});


