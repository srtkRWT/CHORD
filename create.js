const express   = require('express');
const app       = express();
const http      = require('http');

const M         = 4;
const N         = 10;

let successor   = undefined;
let predecessor = undefined;
let myId        = null;
let myPort      = null;
let fingerTable = undefined;

const power2    = [1, 2, 4, 8, 16, 32, 64, 128, 256, 512, 1024, 2048, 4096];

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
            fs_res = parseInt(fs_res, 10);
            fingerTable[fingerIndex] = {
                port  : fs_res,
                id    : fs_res % N
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
            x = parseInt(x, 10);
            if(successor.id == myId || gtAndLt(x % N, myId, successor.id)){ //myId < x && x < successor.id
                successor = {
                    port: x,
                    id: x % N
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
        return (myPort);
    }

    for(let i = M - 1; i >= 0; --i){
        if(fingerTable[i].id == id || gtAndLt(fingerTable[i].id, myId, id)){ // myId < fingerTable[i].id && fingerTable[i].id <= id
           return (fingerTable[i].port);
        }
    }

    return (myPort);
}

////////



app.get('/findSuccessor/:id', (req, res) =>{
    let id = parseInt(req.params.id, 10) % 10;

    if(id == myId){
        return res.send(myPort.toString());
    }

    if(successor.id == myId || gtAndLt(id, myId, successor.id) || id == successor.id){ //(myId < id && id <= successor.id)
        return res.send(successor.port.toString());
    }
    else{
        let cpn = closestPrecedingNode(id);

        if(cpn % N == id){
            return res.send(cpn.toString());
        }

        findSuccessor(cpn, id, function(err, fs_result){
            if(err){
                return res.send(err.toString());
            } else{
                return res.send(fs_result);
            }
        });

        // findSuccessor(successor.port, id, function(err, fs_result){
        //     if(err){
        //         return res.send(err.toString());
        //     } else{
        //         return res.send(fs_result);
        //     }
        // });
    }

});


app.get('/predecessor', (req, res) =>{
    res.send(predecessor.port.toString(10));
});

app.get('/successor', (req, res) =>{
    res.send(successor.port.toString(10));
});

app.get('/join/:port', (req, res) =>{
    let port    = parseInt(req.params.port, 10);
    
    // predecessor = {
    //     port : -1,
    //     id   : -1
    // };
    
    findSuccessor(port, myId, function(err, fs_res){
        if(err){
            console.log(err);
        } else {
            fs_res = parseInt(fs_res, 10);
            successor = {
                port : fs_res,
                id   : fs_res % N
            };
            fingerTable[0] = successor;
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

/////////


if(process.argv.length == 3){
    myPort = parseInt(process.argv[2], 10);
    myId   = myPort % 10;
    
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
        {port : myPort, id : myId}
    ];

    app.listen(myPort);
    
    setInterval(stabilize, 3000);
    //setInterval(fixFinger, 3000);
    
}
