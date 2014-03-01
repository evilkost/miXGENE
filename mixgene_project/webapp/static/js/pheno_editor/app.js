'use strict';

var PhenotypeEditor = angular.module("PhenotypeEditor",
    ['ngCookies', 'ngGrid', 'angularSpinner']
//    ,function ($interpolateProvider) {
//        $interpolateProvider.startSymbol("{$");
//        $interpolateProvider.endSymbol("$}");
//    }
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
    }

    io.send_classes = function(to_send){
        console.log("SENDING: " + to_send);
        $http({
            method: "POST",
            url: "/experiments/" + io.exp_id+ "/blocks/"+
                io.block_uuid +"/actions/update_user_classes_assignment",
            data: angular.toJson(to_send)
        }).success(function(){
            toastr.info("Class assignment was stored ")
        })
    }

    return io;
});

PhenotypeEditor.controller('PhenoCtrl', function($scope, phenoIO){
    $scope.init_done = false;
    $scope.phenoIO = phenoIO;
    $scope.selected_rows = [];

    $scope.available_classes = [];
    $scope.active_class = null;
    $scope.new_class_label = null;
    $scope.fnFilterUserClass = function(header){
        return header.field !== $scope.phenoIO.pheno.user_class_title;
    }


    $scope.gridOptions = {
        data: 'phenoIO.pheno.table',
        columnDefs: 'phenoIO.pheno.headers',
        selectedItems: $scope.selected_rows,
        enableColumnResize: true
//        showFilter: true
    };

    $scope.clean_grid_selection = function(){
        $scope.gridOptions.$gridScope.toggleSelectAll(false);
    }

    $scope.update_available_classes = function(){
        var classes = [];
        var title_field = $scope.phenoIO.pheno.headers_title_to_code_map[
            $scope.phenoIO.pheno.user_class_title];
        angular.forEach($scope.phenoIO.pheno.table, function(value, key){
            if( value[title_field] != null && value[title_field] != undefined){
                classes.push(value[title_field].toString());
            }
        });

        $scope.available_classes = _.unique(classes);
        console.log($scope.available_classes);
    }

    $scope.activate_class_for_assignment = function(class_title){
        console.log("activated: " + class_title);
        $scope.active_class = class_title;
    }

    $scope.add_new_class = function(){
        var new_class = $scope.new_class_label;
        if( new_class != null && new_class != undefined && new_class != ""){
            if(! _.contains($scope.available_classes, new_class)){
                $scope.available_classes.push(new_class);
                $scope.active_class = new_class;
            }
        };
        $scope.new_class_label = null;
    }

    $scope.assign_class = function(){
        var dst_title = $scope.phenoIO.pheno.user_class_title;

        angular.forEach($scope.selected_rows, function(value, key){
            value[dst_title] = $scope.active_class;
        });

        $scope.clean_grid_selection();

    }

    $scope.on_data_fetched = function(){
        $scope.update_available_classes();
        $scope.init_done = true;
    }

    $scope.init = function(exp_id, block_uuid){
        console.log("Initiated phenoIO with exp_id=" + exp_id + " block_uuid=" + block_uuid);
        $scope.phenoIO.init_source(exp_id, block_uuid);
        $scope.phenoIO.fetch_data($scope.on_data_fetched);
    }

    $scope.clone_feature_as_target_class = function(header){
        document._header = header;
        var src_title_field = header.field;
        var dst_title_field = $scope.phenoIO.pheno.headers_title_to_code_map[
            $scope.phenoIO.pheno.user_class_title];

        angular.forEach($scope.phenoIO.pheno.table, function(value, key){
            value[dst_title_field] = value[src_title_field];
        });

        $scope.update_available_classes();
    }

    $scope.save_assignment = function(){
        var classes = []
        var src_title = $scope.phenoIO.pheno.user_class_title;
        angular.forEach($scope.phenoIO.pheno.table, function(value, key){
            classes.push(value[src_title]);
        });
        var to_send = {
            "user_class_title": $scope.phenoIO.pheno.user_class_title,
            "classes": classes
        }

        $scope.phenoIO.send_classes(to_send);
    }




});