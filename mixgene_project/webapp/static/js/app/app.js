'use strict';

//(1)
var Constructor = angular.module("Blog", ["ui.bootstrap", "ngCookies"],
    function ($interpolateProvider) {
        $interpolateProvider.startSymbol("{[{");
        $interpolateProvider.endSymbol("}]}");
    }
);


