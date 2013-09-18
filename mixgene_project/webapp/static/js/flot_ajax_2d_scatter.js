$(function() {
    var options = {
        lines: {
            show: false
        },
        points: {
            show: true
        }
    };

    var data = [];
    $.plot("#placeholder", data, options);

    // Fetch one series, adding to what we already have
    var alreadyFetched = {};
    $(document).ready(function () {
        var button = $(this);
        // Find the URL in the link right next to us, then fetch the data

        var dataurl = $("#flot_source").attr("href");

        function onDataReceived(response) {
            // Extract the first coordinate pair; jQuery has parsed it, so
            // the data is now just an ordinary JavaScript object

            series_list = response['series_list']
            // Push the new data onto our existing data array
            $.each(series_list, function(idx, series){
                if (!alreadyFetched[series.label]) {
                    alreadyFetched[series.label] = true;
                    data.push(series);
                }
            });
            $.plot("#placeholder", data, options);
        }

        $.ajax({
            url: dataurl,
            type: "GET",
            dataType: "json",
            success: onDataReceived
        });
    });


});
