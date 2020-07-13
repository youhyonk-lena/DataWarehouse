//firebase
const firebaseConfig = {
    apiKey: "AIzaSyAo5kY5_b_z0wIplwWfOxvRzYl9x4ymBTU",
    authDomain: "inf551-bb52b.firebaseapp.com",
    databaseURL: "https://inf551-bb52b.firebaseio.com",
    projectId: "inf551-bb52b",
    storageBucket: "inf551-bb52b.appspot.com",
    messagingSenderId: "776211305714",
    appId: "1:776211305714:web:667a23d0ab775278d8ec6b",
    measurementId: "G-88Q4V3JNV8"
};
firebase.initializeApp(firebaseConfig);
fb = firebase.database();
resultDiv = document.getElementById("result");
drpdn =  $("#database");
resultElem = $("#result");
statusElem = $("#status");
searchText = $("#search-text");
searchText.keypress(function (e) {
    if (e.which === 13) {
        $("#search-button").click();
        return false;
    }
})
order = [];

drpdn.keypress(function (e) {
    if (e.which === 13) {
        $("#search-button").click();
        return false;
    }
})

function getKeywords(s) {

    return s.toLowerCase().replace(/[^\w\s]|_/g, " ").replace(/\s+/g, " ");
}


async function submit(input = null){

    used = [];
    start = performance.now();
    if (input === null){
        input = searchText.val();
    }
    let db = drpdn.val();

    if(db && input){

        dbRef = fb.ref(db);
        keywords = getKeywords(input)
        const result = await query(keywords.split(" "));

        if (Object.keys(result).length === 0) {
            alert("No result");
            return false;
        }
        else {
            resultElem.hide();
            resultElem.empty();
            const sortedIndex = sortFreq(result);
            const data = await getData(sortedIndex);
            await displayTable(data);
            expandSearch();
        }

    }
    else{
        alert("MUST SELECT DATABASE AND ENTER KEYWORD");
        return false;
    }

    return false

}

async function query(keywords){

    let idxRef = dbRef.child('index');
    let frequency = {}

    keywords.forEach(function(key){
        idxRef.child(key).on('value', function(snapshot) {
            if (snapshot.val()){
                used.push(key);
                snapshot.val().forEach(function (c) {

                    let table = c.table;
                    let pk = c.primary;

                    if (!(table in frequency)) {
                            frequency[table] = {};
                            frequency[table][pk] = [[key], 1];
                    }
                    else{
                        if (!(pk in frequency[table])) {
                            frequency[table][pk] = [[key], 1]
                        }
                        else {
                            if (frequency[table][pk][0].indexOf(key) === -1) {
                                frequency[table][pk][0].push(key)
                                frequency[table][pk][1] += 200;
                            }
                            else{
                                frequency[table][pk][1] += 15;
                            }
                        }
                    }
                })
            }
            else {
            }
        })
    })

    const promise = new Promise((res, rej) => {
        setTimeout(() => res(frequency), 320)
    })
    return promise


}


function sortFreq(frequency) {

    let allTables = {};
    for (const k of Object.keys(frequency)) {

        let freq = Object.entries(frequency[k]);
        freq.sort(function(first, second) {
            return second[1][1] - first[1][1];
        });

        let sum = 0;
        let pkList = [];
        freq.forEach(function (elem) {
            pkList.push(elem[0]);
            sum += elem[1][1];
        })
        allTables[k] = pkList;
        order.push([k, sum]);
    }
    order.sort(function(a,b){
        return b[1] - a[1];
    });
    return allTables
}


async function getData(result) {

    let allData = {}
    for (const [k, v] of Object.entries(result)) {

        const promises = v.map(function(key) {
            return dbRef.child(k).child(key).once("value");
        })

        let rows = [];
        Promise.all(promises).then(function(snapshots) {
            snapshots.forEach(function(snapshot) {
                const kv = snapshot.val();
                rows.push(kv);
            })
        })
        const rowPromise = Promise.resolve(rows);
        allData[k] = await rowPromise;
    }

    const promise = new Promise((res, rej) => {
        setTimeout(() => res(allData), 320)
    })

    return promise
}


async function displayTable(data) {

    const orderPromise = Promise.resolve(order);
    let o = await orderPromise;
    for (const ent of o){
        let table = ent[0];

        let dic = data[table];

        let id = '#' + table;
        if($(id).length){
            $(id).dataTable.destroy();
            createTable([table, dic]);
        }else{
            createTable([table,dic]);
        }

        const cols = await getLongest(dic)
        const rows = await reformData(dic, cols);

        const header = [];
        for(const c of cols) {
            header.push({"title" : c});
        }

        $(document).ready(function () {
            $(id).dataTable({
                data: rows,
                columns: header,
                "searching": false,
                scrollX: true,
                scrollY: true,
                responsive: true,
                ordering: false,
                autoWidth: true,
                searchHighlight: true,
                "columnDefs": [{"className": "dt-center", "targets": "_all"}]
            })
        })
    }
    let last = resultDiv.lastElementChild;
    $(last).ready(function() {displayTime()});
    resultElem.show();
    expandSearch();
    // return ids
}


function createTable([name, content], paernt = 'result') {

    // const usedPromise = Promise.resolve(used);
    // const u = await usedPromise;

    let newTable = document.createElement("table");
    newTable.id = name;
    newTable.setAttribute('class', 'display');

    let newDiv = document.createElement("h5");
    newDiv.id = name + "Name";
    newDiv.innerHTML = name;
    newDiv.setAttribute("float", "left");

    let statusDiv = document.createElement("h6");
    statusDiv.id = "status";
    statusDiv.innerHTML = "Showing " + Object.keys(content).length + " rows for: " + [...new Set(used)].toString();
    statusDiv.setAttribute("float", "right");

    resultDiv.appendChild(newDiv);
    resultDiv.appendChild(statusDiv);
    resultDiv.appendChild(newTable);
    $('#result').css('background-color', '').css('background-color', 'lightcyan');

    return newDiv;
}

function displayTime(){

    let timeDiv = document.createElement("p");
    timeDiv.id = "time";
    let time = performance.now() - start;
    timeDiv.innerHTML = "search time: " + time + " milliseconds";
    resultDiv.appendChild(timeDiv);
}

async function getLongest(l){

    let cols;
    l.forEach(function (dic){
        let k = Object.keys(dic);
            if(!cols) {
                cols = k;
            }
            else {
                if (cols.length < k.length) {
                    cols = k;
                }
            }
        })

    return Promise.resolve(cols)
}

function reformData(data, cols) {

    let newData = [];
    data.forEach(function (row) {
        let r = [];
        cols.forEach(function  (c) {
            if (c in row) {
                r.push(row[c]);
            }
            else {
                r.push('');
            }
        })
        newData.push(r);
    })
    return newData
}

function expandSearch() {

   $('.display').on('click', 'tbody td', function () {
        submit(this.textContent)
    })

}

