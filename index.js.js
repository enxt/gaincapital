const download = require('download')
      fs = require('fs'),
      d3 = require('d3'),
      AdmZip = require('adm-zip');

const parseWebDates = (content, instr) => {
    let contenta = content.split("\n");
    let c = 0;
    let pairlist = [];
    for(var i in contenta) {
		if(contenta[i].length >= 157 && contenta[i].substring(144,157).indexOf(instr) > -1) {
			pairlist[c++] = contenta[i].substring(144,157);					
		}
    }
    return pairlist;
};

const checkPrev = (pair, year, month, out) => {
    if(!fs.existsSync(out)) {
        fs.mkdirSync(out);
    }
    if(!fs.existsSync(out + "/" + pair)) {
        fs.mkdirSync(out + "/" + pair);
    }

    if(fs.existsSync(out + "/" + pair + "/" + pair + "_" + year + "_" + month + ".json")) {
        return true;
    }

    return false;
};

const extract = function(folder, pair) {//, totalpairs, cb) {
	var zip = new AdmZip(folder + "/" + pair + ".zip");
    zip.extractAllTo(folder);
    return folder + "/" + pair;
};

const joinCSVToJSON = pairlist => {
    let data = []
    for(var j in pairlist) {
        let raw = fs.readFileSync(pairlist[j] + ".csv", 'utf8');
        let csv = d3.csvParse(raw);
        console.log("join con: " + pairlist[j]);
		delete csv.columns;
		for(var i in csv) {
			delete csv[i].lTid
			delete csv[i].cDealable
			delete csv[i].CurrencyPair
			Object.defineProperty(csv[i], "datetime", Object.getOwnPropertyDescriptor(csv[i], "RateDateTime"));
			delete csv[i].RateDateTime			
			Object.defineProperty(csv[i], "bid", Object.getOwnPropertyDescriptor(csv[i], "RateBid"));
			delete csv[i].RateBid
			Object.defineProperty(csv[i], "ask", Object.getOwnPropertyDescriptor(csv[i], "RateAsk"));
			delete csv[i].RateAsk

			csv[i].datetime = (new Date(csv[i].datetime)).getTime();
			csv[i].ask = parseFloat(csv[i].ask);
			csv[i].bid = parseFloat(csv[i].bid);

            data.push(csv[i]);
        }

        fs.unlinkSync(pairlist[j] + '.csv');
    }

    return JSON.stringify(data);
};

module.exports = (pair, year, month, out, callback) => {
    const host = "http://ratedata.gaincapital.com",
          port = 80;

    String.prototype.padRight = function(l,c) {return this+Array(l-this.length+1).join(c||" ")}
    String.prototype.padLeft  = function(l,c) {return Array(l-this.length+1).join(c||" ")+this}
    Date.prototype.getMonthName = function(){ return (["January","February","March","April","May","June","July","August","September","October","November","December"])[this.getMonth()]; }
    
    const paddedmonth = (""+month).padLeft(2, "0");
    const date = new Date(year+"-"+paddedmonth);
    let urlpath = "/" + year + "/" + paddedmonth + "%20" + date.getMonthName() + "/";
    let url = host + ":" + port + urlpath;


    if(!checkPrev(pair, year, paddedmonth, out)) {
        console.log("Downloading: " + url);

        download(url)
        .then(content => {
            content += ""; // to text
            let pairlist = parseWebDates(content, pair);            
            return pairlist;
        })
        .then(pairlist => {
            let promises = [];
            pairlist.forEach(i => {
                let nurl = url + i + ".zip";
                console.log("Downloading:" + nurl)
                let p = new Promise((resolve, reject) => {
                    download(nurl, "./" + out + "/" + pair)
                    .then(() => {
                        let bfile = extract("./" + out + "/" + pair, i);
                        fs.unlinkSync(bfile + ".zip");
                        resolve(bfile);
                    });
                });
                promises.push(p);
            });
            return Promise.all(promises).then(values => {
                return values;
            });
        })
        .then(pairlist => {
            let data = joinCSVToJSON(pairlist);
            let destfile = pair + "_" + year + paddedmonth + ".json";
            fs.writeFileSync("./" + out + "/" + pair + "/" + destfile, data, "utf8");
            callback(destfile);
        });
    } else {
        let destfile = pair + "_" + year + paddedmonth + ".json";
        callback(destfile);
    }
}