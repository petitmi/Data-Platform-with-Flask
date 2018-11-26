// JavaScript Document
var Util={
    windowWidth:350,
    windowHeight:480,
    selfPlaneElement:null,
    enemyPlaneElement:null,
    bulletElement:null,
    parentElement:null,
    scoreSpan:null,
    g:function(id){
        return document.getElementById(id);
    },

    init:function(){
        this.parentElement=this.g("parent");

        this.selfPlaneElement=this._loadImg("static/img/shit_white.jpeg");

        this.enemyPlaneElement=this._loadImg("static/img/shit_white.jpeg");

        this.bulletElement=this._loadImg("static/img/shit_green_small.jpg");

        this.scoreSpan=this.g("score");
    },

    _loadImg:function(src){
        var e=document.createElement("img");
        e.style.position="absolute";
        e.src=src;
        return e;
    }
}