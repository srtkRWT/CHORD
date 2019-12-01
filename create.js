const express   = require('express');
const app       = express();
const http      = require('http');
const sha       = require('sha1');
const BigInt    = require('big-integer');

const M         = 11;
const N         = 4096;

let successor   = undefined;
let predecessor = undefined;
let myId        = null;
let myPort      = null;
let fingerTable = undefined;

const power2    = [1, 2, 4, 8, 16, 32, 64, 128, 256, 512, 1024, 2048, 4096];


/************** */

let psudoDataBase = [];

/************** */
function gtAndLt(key, lowerBound, upperBound){
    let d1 = (key - lowerBound + N) % N + N % N;
    let d2 = (upperBound - lowerBound + N) % N + N % N;
    
    if(d1 != 0 && d2 != 0 && d1 < d2)
        return true;
    return false;
}

function increment(value, offset){
    return (value + offset) % N;
}

function decrement(value, offset){
    offset = offset % N;
    return (value - offset + N) % N;
}

function printDetails(){
    console.log(fingerTable);
    console.log(successor);
    console.log(predecessor);
}

function getId(msg){
    let hash = sha(msg);
    let id   = BigInt(hash, 16).mod(N);
    console.log(id);
    return id.valueOf();
}


function sendRequest(port, url, next){
    var options = {
        host: 'localhost',
        port: port,
        path: url
    };

    var request = http.get(options, function(result) {
        var bodyChunks = [];
        result.on('data', function(chunk) {
            bodyChunks.push(chunk);
        }).on('end', function() {
            var body = Buffer.concat(bodyChunks);
            //console.log(options.port + ' BODY: ' + body);
            next(null, bodyChunks.toString());
        })
    });

    request.on('error', function(e) {
        console.log('ERROR: ' + e.message);
        next(e, null);  
    });
}

function findSuccessor (port, id, next){
    sendRequest(port, '/findSuccessor/' + id, function(err, sr_res){
        if(err){
            console.log(err);
            next(err, null);
        } else {
            next(null, sr_res);
        }
    });
}

function getSuccessor(port, next){
    sendRequest(port, '/successor', function(err, sr_res){
        if(err){
            console.log(err);
            next(err, null);
        } else {
            next(null, sr_res);
        }
    });
}

function getPredecessor(port, next){
    sendRequest(port, '/predecessor', function(err, sr_res){
        if(err){
            console.log(err);
            next(err, null);
        } else {
            next(null, sr_res);
        }
    });
}

function notify(port, n_port, n_id){
    let url =  '/notify/' + n_port + '/' + n_id;
    sendRequest(port, url, function(err, sr_res){
        if(err){
            console.log(err);
        }
    });
}

let fingerIndex = 0;
function fixFinger(){
    console.log('fixing fingers!!');
    //let crr   = fingerTable[fingerIndex];
    let crrId = increment(myId, power2[fingerIndex]);
    findSuccessor(myPort, crrId, function(err, fs_res){
        if(err){
            console.log(err);
        } else {
            //fs_res = parseInt(fs_res, 10);
            fs_res = JSON.parse(fs_res);
            fingerTable[fingerIndex] = {
                port : fs_res.port,
                id   : fs_res.id
            };
            fingerIndex = (fingerIndex + 1) % M;
        }
    });
}

function stabilize(){
    getPredecessor(successor.port, function(err, x){
        if(err){
            console.log(err);
        } else {
            //x = parseInt(x, 10);
            x = JSON.parse(x);

            if(successor.id == myId || gtAndLt(x.id, myId, successor.id)){ //myId < x && x < successor.id
                successor = {
                    port : x.port,
                    id   : x.id
                };
            }

            notify(successor.port, myPort, myId);
            
            fixFinger();

            printDetails();
        }
    });
    
}

function closestPrecedingNode(id){
    if(id == myId || myId == successor.id){
        return ({port : myPort, id : myId});
    }

    for(let i = M - 1; i >= 0; --i){
        if(fingerTable[i].id == id || gtAndLt(fingerTable[i].id, myId, id)){ // myId < fingerTable[i].id && fingerTable[i].id <= id
           return ({port : fingerTable[i].port, id : fingerTable[i].id});
        }
    }

    return ({port : myPort, id : myId});
}

////////



app.get('/findSuccessor/:id', (req, res) =>{
    let id = parseInt(req.params.id, 10) % 10;

    if(id == myId){
        return res.json({
            port : myPort,
            id   : myId,
            err  : null 
        });
    }

    if(successor.id == myId || gtAndLt(id, myId, successor.id) || id == successor.id){ //(myId < id && id <= successor.id)
        return res.json({
            port : successor.port,
            id   : successor.id,
            err  : null 
        });
    }
    else{
        let cpn = closestPrecedingNode(id);

        if(cpn.id == id){
            return res.json({
                port : cpn.port,
                id   : cpn.id,
                err  : null
            });
        }

        findSuccessor(cpn.port, id, function(err, fs_result){
            if(err){
                return res.json({err : err.toString()});
            } else{
                fs_result = JSON.parse(fs_result);
                return res.json(fs_result);
            }
        });
    }
});


app.get('/predecessor', (req, res) =>{
    res.json({
        port : predecessor.port,
        id   : predecessor.id,
        err  : null 
    });
});
app.get('/successor', (req, res) =>{
    res.json({
        port : successor.port,
        id   : successor.id,
        err  : null 
    });
});

app.get('/join/:port', (req, res) =>{
    let port    = parseInt(req.params.port, 10);
    
    findSuccessor(port, myId, function(err, fs_res){
        if(err){
            console.log(err);
        } else {
            fs_res = JSON.parse(fs_res);
            successor = fs_res;
            fingerTable[0] = fs_res;
        }
        res.send('joined node');
    });

});

app.get('/notify/:port/:id', (req, res) =>{
    let newid   = parseInt(req.params.id, 10);
    let newport = parseInt(req.params.port, 10);
    if (predecessor.id == myId || gtAndLt(newid, predecessor.id, myId)){ // (predecessor.id < id && id < myId)
        predecessor = {
            port    : newport,
            id      : newid
        };
    }

    console.log(myPort + ' notified by ' + newport);

    res.send('notify page....');
});

// app.get('/store/:key/:value', (req, res) => {
//     let key   = parseInt(req.param.key, 10);
//     let value = parseInt(req.param.value, 10);

//     let id    = getId(req.param.key);

//     findSuccessor(myPort, id, (err, fs_res) => {
//         if(err){
//             console.log(err);
//             return res.send(err);
//         } else {
//             fs_res = JSON.parse(fs_res);
//             storeUtil(fs_res.port, key, value, id, (err, su_res) => {
//                 if(err){
//                     console.log(err);
//                     return res.send(err);
//                 } else {
//                     return res.send(su_res);
//                 }
//             });
//         }
//     });
    
// });

// app.get('/store_util/:key/:value/:id', (req, res) => {
//     let key   = parseInt(req.param.key, 10);
//     let value = parseInt(req.param.value, 10);
//     let id    = parseInt(req.param.id, 10);

//     if(id != myId){
//         res.send('err... id miss-match');
//     } else {
//         psudoDataBase.push({id : id, key : key, value : value});
//     }

// });

/////////


if(process.argv.length == 3){
    myPort = parseInt(process.argv[2], 10);
    myId   = getId(myPort.toString());
    
    successor = {
        port : myPort,
        id   : myId
    };

    predecessor = {
        port : myPort,
        id   : myId
    };
    
    fingerTable = [
        {port : myPort, id : myId},
        {port : myPort, id : myId},
        {port : myPort, id : myId},
        {port : myPort, id : myId},
        {port : myPort, id : myId},
        {port : myPort, id : myId},
        {port : myPort, id : myId},
        {port : myPort, id : myId},
        {port : myPort, id : myId},
        {port : myPort, id : myId},
        {port : myPort, id : myId}
    ];

    app.listen(myPort);
    
    setInterval(stabilize, 3000);
}
