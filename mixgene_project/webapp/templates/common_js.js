function bootstrapify_datatable(datatable){
    var search_input = datatable.closest('.dataTables_wrapper').find('div[id$=_filter] input');
    search_input.attr('placeholder', 'Search');
    search_input.addClass('form-control input-sm');
    // LENGTH - Inline-Form control
    var length_sel = datatable.closest('.dataTables_wrapper').find('div[id$=_length] select');
    length_sel.addClass('form-control input-sm');
}

function render_table($handle, df_data){
    // $handle JQuery dom selector
    // $df_data  python pandas.DataFrame.to_json(orient="split")
    //  has 3 fields: index, data, columns
    $handle.html("<table class='table datatable'><thead></thead><tbody></tbody></table>");

    var $table = $handle.find('table');
    var $table_head = $handle.find('table thead');

    var $th = $("<tr><td></td></tr>"); // FIXME: compare columns with first line of data to decide about index column
    $.each(df_data.columns, function(idx, obj){
        var $td = $("<td> "+obj+"</td>");
        $th.append($td);
    })
    $table_head.append($th);
    $table.dataTable({
        "sPaginationType": "bs_normal",
        "sScrollX": "auto",
        "bAutoWidth": false,
        "sScrollY": "400px",
        "bScrollCollapse": true
    });
    var rows_num = df_data.index.length;
    for(var i=0; i<rows_num; i++){
        var datarow =  [df_data.index[i]].concat(df_data.data[i]);
        $table.dataTable().fnAddData(datarow);
    }
    bootstrapify_datatable($table);
}

function render_plot_2d($handle, df_data){
    var options = {
        lines: {
            show: false
        },
        points: {
            show: true
        }
    };

    $.plot($handle, [df_data.data], options);
}


