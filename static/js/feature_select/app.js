'use strict';

var FeatureSelector = angular.module("FeatureSelector",
    ['ngCookies', 'ngTable', 'angularSpinner', 'ui.bootstrap']
    ,function ($interpolateProvider) {
        $interpolateProvider.startSymbol("{$");
        $interpolateProvider.endSymbol("$}");
    }
);

FeatureSelector.run(function ($http, $cookies) {
    $http.defaults.headers.common['X-CSRFToken'] = $cookies['csrftoken'];
});

FeatureSelector.factory("phenoIO", function($http){
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

    io.send_classes = function(method_name, to_send, on_success){
        console.log("SENDING: " + angular.toJson(to_send));
        $http({
            method: "POST",
            url: "/experiments/" + io.exp_id+ "/blocks/"+
                io.block_uuid +"/actions/" + method_name,
            data: angular.toJson(to_send)
        }).success(function(){
            on_success();
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

FeatureSelector.controller('PhenoCtrl', function($scope, phenoIO, $modal, $filter, ngTableParams){
    $scope.init_done = false;
    $scope.phenoIO = phenoIO;
    $scope.features = [];

    var table_config = {};
    $scope.table_config = table_config;
    table_config["filter_dict"] = {};
    table_config["data"] = [];
    table_config["columns"] = [];
    table_config["tableParams"] = null;

    $scope.table_config.tableParams = new ngTableParams({
        page: 1,            // show first page
        count: 10          // count per page
        //        ,debugMode: false
    }, {
        total: $scope.table_config.data.length, // length of data
        getData: function($defer, params) {
            if(!$scope.init_done){
                var orderedData=[];
            } else {
                var filteredData = $scope.table_config.filter_dict ?
                    $filter('filter')($scope.table_config.data, $scope.table_config.filter_dict) :
                    $scope.table_config.data;

                // use build-in angular filter
                var orderedData = params.sorting() ?
                    $filter('orderBy')(filteredData, params.orderBy()) :
                    filteredData;
            }
            params.total(orderedData.length); // set total for recalc pagination
            $defer.resolve(orderedData.slice((params.page() - 1) * params.count(), params.page() * params.count()));
        }
    });

    $scope.toggle_sorting = function(column){
        $scope.table_config.tableParams.sorting(
            column.field,
            $scope.table_config.tableParams.isSortBy(column.field, 'asc') ? 'desc' : 'asc'
        );
    };

    $scope.on_data_fetched_ng = function(){
        _.each($scope.phenoIO.pheno.headers, function(header){
            console.log(header.displayName);
            $scope.table_config.filter_dict[header.field]  = "";
            $scope.table_config.columns.push(
                {title: header.displayName, field: String(header.field), visible: true}
            );
        });
        $scope.table_config.data = $scope.phenoIO.pheno.table;
    };

    $scope.on_data_fetched = function(){
        _.each($scope.phenoIO.pheno.headers, function(header){

            $scope.features.push({
                name: header.displayName,
                active: _.contains($scope.phenoIO.pheno.features, header.displayName)
            })

        });
        $scope.init_done = true;
        $scope.on_data_fetched_ng();
        $scope.table_config.tableParams.reload();

        console.log("Initiated phenoIO with exp_id=" + $scope.phenoIO.exp_id +
            " block_uuid=" + $scope.phenoIO.block_uuid);
    };

    $scope.init = function(exp_id, block_uuid){
        console.log("Initiating phenoIO with exp_id=" + exp_id + " block_uuid=" + block_uuid);
        $scope.phenoIO.init_source(exp_id, block_uuid);
        $scope.phenoIO.fetch_data($scope.on_data_fetched);
    };

    $scope.switch_selection = function(feature){
        feature.active = !feature.active;
    };

    $scope.select_all = function(val){
        _.each($scope.features, function(feature){
            feature.active = val;
        });
    }

    $scope.save_selection = function(){
        var feature_list = [];
        _.each($scope.features, function(feature){
            if(feature.active == true){
                feature_list.push(feature.name);
            }
        });
        var to_send = {
            features: feature_list
        };
        $scope.phenoIO.send_classes("update_feature_selection", to_send, function(){
            toastr.info("Feature selection was stored ")
        });
    }

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