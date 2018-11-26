   function fun(obj){
    var div = document.getElementById("hide");
    if(obj.value=="隐藏"){
        div.style.display = "none";
        obj.value = "显示";
    } else {
        div.style.display = "block";
        obj.value = "隐藏";
    }
}