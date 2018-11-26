    // var $ = function(id){return document.getElementById(id)};
    var minesweeper = {};   //雷区对象
    var mineArray = [];     //地雷组
    var _differendNumber = null;    //生成不同位置的随机地雷
    var overcomeMineLenth = {overcome:0, minelength:10};


    function CreateBox(y,x,mine){   //构造雷格对象
        this.position = 1;     //1在雷区里面，0不在
        this.y = y;
        this.x = x;
        this.mine = mine;  //1有雷，0无
        this.on = 0;   //0未扫，1已扫，2已标记
        this.t_l = {y:y-1, x:x-1};     //左上格子坐标
        this.t_c = {y:y-1, x:x};       //上
        this.t_r = {y:y-1, x:x+1};     //右上
        this.b_l = {y:y+1, x:x-1};     //左下
        this.b_c = {y:y+1, x:x};       //下
        this.b_r = {y:y+1, x:x+1};     //右下
        this.l   = {y:y, x:x-1};       //左
        this.r   = {y:y, x:x+1};       //右
    }

    function getMineSweeper(){
        var oCount = parseInt(document.getElementById('oCount').innerHTML);
        var y, x, max, boxslist=[],has=[];
        switch(oCount){    //取得雷区长宽
            case 10: y = 9; x = 9;
            break;
            case 40: y = 16; x = 16;
            break;
            case 99: y = 16; x = 30;
            break;
        }
        max = y*x;
        document.getElementById('oMain').style.width = 24*x+2+'px';
        for(var i=0; i<y; i++){    //生成格子
            boxslist[i] = [];
            for(var j=0; j<x; j++){
                boxslist[i].push(new CreateBox(i,j,0));
            }
        }
        for(var k=0; k<oCount; k++){   //生成随机雷格数组序号
            var new_num = Math.floor(Math.random()*max+1);
            getDifferentNumber(new_num,has,max);
            new_num = _differendNumber;
            has.push(new_num);
        }
        function setMine(arr){
            for(var i=0; i<has.length; i++){
                var mineX = arr[i] % x == 0 ? x : arr[i] % x;
                var mineY = Math.ceil(arr[i] / x);
                boxslist[mineY-1][mineX-1].mine = 1;
            }
        }
        mineArray = has;
        overcomeMineLenth.minelength = mineArray.length;
        setMine(has);
        function getDifferentNumber(n,has,max){    //生成不重复的随机数
            var _has = has;
            var _max = max;
            var _n = n;
            var isin = 0;
            for(var i=0; i<_has.length; i++){
                if(_n==has[i]){
                   isin = 1;
                }
            }
            if(isin==1){   //如果相同，递归执行
                var _num = Math.floor(Math.random()*_max+1);
                arguments.callee(_num,_has,_max);
            }else{
                _differendNumber = _n;
            }
        }
        minesweeper.len = max;
        minesweeper.x = x;
        minesweeper.y = y;
        minesweeper.boxslist = boxslist;   //雷格数组
    }
    function mineSweeperToHTML(obj){    //布雷到页面HTML
         var x_len = obj.x;
         var y_len = obj.y;
         var arr = obj.boxslist;
         var str = '';
         for(var i=0; i<arr.length; i++){
             var _tr = '<tr>';
             for(var j=0; j<arr[i].length; j++){
                 var id = 'box-' + i + '-' + j ;
                 var _mineclass = 'shit-hide';   //arr[i][j].mine==0?'':'mine'
                 _tr += '<td id="' + id + '" class="' + _mineclass + '">&nbsp;</td>';
             }
             _tr += '</tr>';
             str += _tr ;
         }
         document.getElementById('mineField').innerHTML = '<table>' + str + '</table>';
         //添加事件
         var boxs = document.getElementById('mineField').getElementsByTagName('td');
         var len = boxs.length;
         for(var k=0; k<len; k++){
             boxs[k].onclick = function(){
                 mineBoxClick(this);
             }
             boxs[k].onmousedown = function(event){
                 addFlag(this,event);
             }
         }
    }

    function mineBoxClick(ele){     //格子点击
        var ele_id = ele.getAttribute('id');
        var ele_id_arr = ele_id.split('-');
        var y = parseInt(ele_id_arr[1]);
        var x = parseInt(ele_id_arr[2]);
        var box = minesweeper.boxslist[y][x];
        var eleclass = ele.getAttribute('class');
        if(eleclass!='shit-hide'){return false;}   //如果已经点击，返回false
        // var ele = document.getElementById('box-'+y+'-'+x);
        box.on = 1;
        if(box.mine==0){    //不是雷
            overcomeMineLenth.overcome += 1;
            var tl,tc,tr,bl,bc,br,l,r;  //获取周围的格子对象
            var out = {position:0, mine:0};
            var maxY = minesweeper.y - 1;
            var maxX = minesweeper.x - 1;
            if(box.t_l.y < 0 || box.t_l.y > maxY || box.t_l.x < 0 || box.t_l.x > maxX){
                tl = out ;
            }else{
                tl = minesweeper.boxslist[box.t_l.y][box.t_l.x];
            }
            if(box.t_c.y < 0 || box.t_c.y > maxY || box.t_c.x < 0 || box.t_c.x > maxX){
                tc = out ;
            }else{
                tc = minesweeper.boxslist[box.t_c.y][box.t_c.x];
            }
            if(box.t_r.y < 0 || box.t_r.y > maxY || box.t_r.x < 0 || box.t_r.x > maxX){
                tr = out ;
            }else{
                tr = minesweeper.boxslist[box.t_r.y][box.t_r.x];
            }
            if(box.b_l.y < 0 || box.b_l.y > maxY || box.b_l.x < 0 || box.b_l.x > maxX){
                bl = out ;
            }else{
                bl = minesweeper.boxslist[box.b_l.y][box.b_l.x];
            }
            if(box.b_c.y < 0 || box.b_c.y > maxY || box.b_c.x < 0 || box.b_c.x > maxX){
                bc = out ;
            }else{
                bc = minesweeper.boxslist[box.b_c.y][box.b_c.x];
            }
            if(box.b_r.y < 0 || box.b_r.y > maxY || box.b_r.x < 0 || box.b_r.x > maxX){
                br = out ;
            }else{
                br = minesweeper.boxslist[box.b_r.y][box.b_r.x];
            }
            if(box.l.y < 0 || box.l.y > maxY || box.l.x < 0 || box.l.x > maxX){
                l = out ;
            }else{
                l = minesweeper.boxslist[box.l.y][box.l.x];
            }
            if(box.r.y < 0 || box.r.y > maxY || box.r.x < 0 || box.r.x > maxX){
                r = out ;
            }else{
                r = minesweeper.boxslist[box.r.y][box.r.x];
            }
            var round = tl.mine + tc.mine + tr.mine + bl.mine + bc.mine + br.mine + l.mine + r.mine;
            switch(round){  //周围格子元素样式设置
                case 8:ele.setAttribute('class','on');ele.innerHTML = 8 ;
                break;
                case 7:ele.setAttribute('class','on');ele.innerHTML = 7 ;
                break;
                case 6:ele.setAttribute('class','on');ele.innerHTML = 6 ;
                break;
                case 5:ele.setAttribute('class','on');ele.innerHTML = 5 ;
                break;
                case 4:ele.setAttribute('class','on');ele.innerHTML = 4 ;
                break;
                case 3:ele.setAttribute('class','on');ele.innerHTML = 3 ;
                break;
                case 2:ele.setAttribute('class','on');ele.innerHTML = 2 ;
                break;
                case 1:ele.setAttribute('class','on');ele.innerHTML = 1 ;
                break;
                default:
                ele.setAttribute('class','on');     //如果有空格，继续搜索
                if(tl.position!=0 && tl.on==0) {document.getElementById('box-' + tl.y + '-' + tl.x).click()}
                if(tc.position!=0 && tc.on==0) {document.getElementById('box-' + tc.y + '-' + tc.x).click()}
                if(tr.position!=0 && tr.on==0) {document.getElementById('box-' + tr.y + '-' + tr.x).click()}
                if(bl.position!=0 && bl.on==0) {document.getElementById('box-' + bl.y + '-' + bl.x).click()}
                if(bc.position!=0 && bc.on==0) {document.getElementById('box-' + bc.y + '-' + bc.x).click()}
                if(br.position!=0 && br.on==0) {document.getElementById('box-' + br.y + '-' + br.x).click()}
                if(l.position!=0 && l.on==0) {document.getElementById('box-' + l.y + '-' + l.x).click()}
                if(r.position!=0 && r.on==0) {document.getElementById('box-' + r.y + '-' + r.x).click()}
            }
            if(overcomeMineLenth.overcome+overcomeMineLenth.minelength == minesweeper.len){
                alert('恭喜，你避开了所有翔！');
                document.getElementById('start').onclick();
            }
        }
        else{  //是雷
            for(var i=0; i<mineArray.length; i++){
                var mineElement = document.getElementById('mineField').getElementsByTagName('td');
                mineElement[mineArray[i]-1].setAttribute('class','mine');
            }
            ele.setAttribute('class','boom');
            document.getElementById('start').setAttribute('src','/static/img/shit_green.jpeg');
            //移除添加事件
             var boxs2 = document.getElementById('mineField').getElementsByTagName('td');
             var len = boxs2.length;
             for(var k=0; k<len; k++){
                 boxs2[k].onclick = function(){
                     return false;  //mineBoxClick(this);
                 }
                 boxs2[k].onmousedown = function(event){
                     return false;  //addFlag(this,event);
                 }
             }
        }
    }


    function addFlag(ele,event){    //标记红旗
        var e = event || window.event;

        if(e.button=='2'){
            // e.cancelBubble = true
            // e.returnvalue = false;
            var _c = ele.getAttribute('class');
            if(_c=='shit-hide'){
                ele.setAttribute('class','flag');
                document.getElementById('oCount').innerHTML = parseInt(document.getElementById('oCount').innerHTML) - 1;
            } else if(_c=='flag'){
                ele.setAttribute('class','shit-hide');
                document.getElementById('oCount').innerHTML = parseInt(document.getElementById('oCount').innerHTML) + 1;
            } else{return false;
            }
        }
    }

    function startGame(){   //开始游戏
        getMineSweeper();
        mineSweeperToHTML(minesweeper);
        overcomeMineLenth.overcome = 0;
    }

    function chooseDifficulty (){   //难度选择
        var arr = document.getElementsByName('radio');
        var len = arr.length;
        for(var i=0; i<len; i++){
        arr[i].onclick = function(){
        document.getElementById('oCount').innerHTML = this.value;
        document.getElementById('start').setAttribute('src','/static/img/shit_green.jpeg');
        startGame();
        }
        }
    }
    function getDifficulty(){   //获取已选难度
        var arr = document.getElementsByName('radio');
        var len = arr.length;
        for(var i=0; i<len; i++){
            if(arr[i].checked){
                document.getElementById('oCount').innerHTML = arr[i].value;
            }
        }
    }
    window.onload = function(){
        chooseDifficulty ()
        startGame();
        document.getElementById('start').onclick = function(){
        this.setAttribute('src','/static/img/shit_green.jpeg');
        getDifficulty();
        startGame();
        }
        document.getElementById('oMain').style.display = 'block';
        document.getElementById('loading').style.display = 'none';
        document.oncontextmenu = function(){ return false;}
    }
