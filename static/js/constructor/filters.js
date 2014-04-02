Constructor.filter('labels_preview', function () {
    return function(input, max_words){
        if( typeof(max_words) != 'number'){
            max_words = 3;
        }

        var list = [];
        if(input.length > max_words){
            _.each(_.range(max_words), function(idx){
                list.push(input[idx]);
            })
            list.push("...")
        } else {
            list = input;
        }
        return list.join(", ")

    };

});