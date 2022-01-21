var request = require("request");
var cheerio = require("cheerio");
var mysql = require('mysql');
var fetch = require('node-fetch');
var _ = require('lodash');
var { mysql_config, crawler_list } = require('./config.json');
var htmlDecode = require('decode-html');
const OPENSEA_APIKEY = 'e5f9d19ffd714d2cacd4bed8bb58b890';

/**
 * 連接資料庫
 */
const connection = mysql.createConnection({
    host: mysql_config.host,
    user: mysql_config.user,
    password: mysql_config.password,
    database: mysql_config.database
});

connection.connect(function (err) {
    if (err) err;

    console.log("數據庫連接成功");
    startFetchTransactions(); //開始爬蟲
});

/**
 * 處理txn
 */
async function processTransaction(data) {
    let grouped = _.groupBy(data, 'token_contract_address');

    console.log('正在寫入...');

    for (const key in grouped) {
        const nft_data = { nft_name: grouped[key][0]['nft_name'], contract_address: key }

        let holders = 0;
        let supplys = 0;

        // if (grouped[key].length > 5) { //mint超過10個才更新總數
        //     supplys = await getTokenSupplys(key);
        //     if (supplys > 1000) { //總數超過1000才抓取 holders
        //         holders = await getTokenHolders(key);
        //     }
        // }

        // let nft_item_id = await checkNftItemExist(nft_data, holders, supplys);
        // if (nft_item_id == 0) {
        //     if (holders != 0) {
        //         await updateNftItemHolders(nft_data, holders);
        //     }
        //     if (supplys != 0) {
        //         await updateNftItemSupplys(nft_data, supplys);
        //     }
        //     nft_item_id = await getNftItemId(nft_data, holders, supplys);
        // }

        let opensea_info = await getOpenseaInfo(nft_data);

        if (opensea_info.hasOwnProperty('collection')) {
            await updateNftInfo(opensea_info.collection, nft_data);
            console.log('已更新 Collection 資料...');
        }

        // let insert = await insertTransaction(nft_item_id, grouped[key]);
    }


    console.log('已完成本次寫入，準備進行下一次抓取...');
    await deleteOldTransaction(); //清除舊紀錄
    console.log('已清除24小時前 Transactions');

    startFetchTransactions(); //開始下次
}

/**
 * 檢查nft_item存在
 */
async function checkNftItemExist(item, holders, supplys) {
    return new Promise(function (resolve, reject) {
        var sql = "INSERT IGNORE INTO nft_item (contract_address, name) VALUES ?";
        var values = [
            [item.contract_address, htmlDecode(item.nft_name)]
        ];
        connection.query(sql, [values], function (err, result) {
            if (err) reject(err);
            resolve(result.insertId);
        });
    })
}

/**
 * 取得nft_item_id
 */

async function getNftItemId(item) {
    return new Promise(function (resolve, reject) {
        connection.query(`SELECT id FROM nft_item WHERE contract_address = '${item.contract_address}' LIMIT 1`, function (err, result) {
            if (err) reject(err);
            resolve(result[0].id);
        });
    })
}

/**
 * 更新 supply
 */
async function updateNftItemSupplys(item, supplys) {
    return new Promise(function (resolve, reject) {
        connection.query(`UPDATE nft_item SET supplys = ${supplys} WHERE contract_address = '${item.contract_address}'`, function (err) {
            if (err) reject(err);
            resolve(true);
        });
    })
}
/**
 * 更新 holders
 */
async function updateNftItemHolders(item, holders) {
    return new Promise(function (resolve, reject) {
        connection.query(`UPDATE nft_item SET holders = ${holders} WHERE contract_address = '${item.contract_address}'`, function (err) {
            if (err) reject(err);
            resolve(true);
        });
    })
}
/**
 * 更新 collection 資料
 */
async function updateNftInfo(info, nft_data) {
    return new Promise(function (resolve, reject) {
        const time = Math.round(new Date(info.created_date).getTime() / 1000);
        connection.query(`UPDATE nft_item SET 
        name = '${info.name}', 
        description = '${mysql_real_escape_string(info.description)}', 
        image_url = '${info.image_url}',
        banner_url = '${info.banner_image_url}',
        official_url = '${info.external_url}',
        discord_url = '${info.discord_url}',
        instagram = '${info.instagram_username}',
        twitter = '${info.twitter_username}',
        opensea_slug = '${info.slug}',
        create_timestamp = '${time + 28800}'
        WHERE contract_address = '${nft_data.contract_address}'`, function (err, rows, fields) {
            if (err) reject(err);
            resolve(true);
        });
    })
}

/**
 * 取得 Opensae collection 資料
 */
async function getOpenseaInfo(item) {
    return new Promise(function (resolve, reject) {

        const options = {
            method: 'GET', headers: { 'X-API-KEY': OPENSEA_APIKEY }
        };

        fetch(`https://api.opensea.io/api/v1/asset_contract/${item.contract_address}`, options)
            .then(response => response.json())
            .then(response => resolve(response))
            .catch(err => reject(err));
    });
}

/**
 * 寫入txn
 */
async function insertTransaction(nft_item_id, list) {
    return new Promise(function (resolve, reject) {
        let values = [];

        list.forEach(item => {
            let temp = [nft_item_id, item.txn_hash, item.token_id, item.timestamp];
            values.unshift(temp);
        });

        let sql = "INSERT IGNORE INTO mint_transactions (nft_item_id, txn_hash, token_id, timestamp) VALUES ?";
        connection.query(sql, [values], function (err) {
            if (err) reject(err);
            resolve(true)
        });
    });
}


/**
 * 刪除24小時之前的數據
 */
async function deleteOldTransaction(nft_item_id, list) {
    return new Promise(function (resolve, reject) {

        const now = Math.floor(Date.now() / 1000);

        const timestamp_limit = now - 86400; //60秒之前

        let sql = `DELETE FROM mint_transactions WHERE timestamp < '${timestamp_limit}'`;
        connection.query(sql, function (err) {
            if (err) reject(err);
            resolve(true)
        });
    });
}



/**
 * 爬蟲程序開始
 */
async function startFetchTransactions() {
    const MAX_PAGE = 5;
    const ITEM_PER_PAGE = 100;

    let transactions = [];

    for (let i = 0; i < MAX_PAGE; i++) {
        let CURRENT_PAGE = i + 1;
        const url = `${crawler_list[0]}?ps=${ITEM_PER_PAGE}&p=${CURRENT_PAGE}`;

        let result = await transactionsCrawler(url);
        transactions = [...transactions, ...result];
        console.log(`正在抓取第${CURRENT_PAGE}頁`);
    }

    processTransaction(transactions);
}

/**
 * 爬蟲程序 core
 */
async function transactionsCrawler(url) {
    return new Promise(function (resolve, reject) {
        request({
            url: url,
            headers: {
                'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/97.0.4692.71 Safari/537.36'
            },
            method: "GET"
        }, (error, res, body) => {
            // 如果有錯誤訊息，或沒有 body(內容)，就 return
            if (error || !body) {
                reject(err);
            }

            const data = [];
            const $ = cheerio.load(body); // 載入 body
            const list = $("table tr");
            for (let i = 1; i < list.length; i++) {
                const td = list.eq(i).find('td');

                let from = td.eq(4).find('a').text();
                let tokenId = td.eq(7).find('a').text();

                if (!from.match('Null Address')) { //非鑄造
                    continue;
                } else if (parseInt(tokenId) > 50000) { //token數量過多，暫時排除
                    continue;
                } else {
                    let txn_hash = td.eq(1).find('a').text();
                    let timestamp = td.eq(2).find('span').text();
                    timestamp = Math.round(new Date(timestamp).getTime() / 1000);
                    timestamp = timestamp + 28800;

                    let token_id = parseInt(td.eq(7).find('a').text());
                    let token = td.eq(8).find('a');
                    let token_contract_address = token.attr('href').split('/')[2];
                    let long_name = token.find('span').find('font');
                    let nft_name = '';

                    // NFT名稱處理
                    if (long_name.length > 0) {
                        nft_name = token.find('span').attr('title');
                    } else {
                        nft_name = token.clone().children().remove().end().text();
                        nft_name = nft_name.split("(");
                        nft_name = nft_name[0];
                        nft_name = nft_name.trim();
                    }

                    if (nft_name != '') {
                        data.push({ nft_name, token_contract_address, txn_hash, token_id, timestamp });
                    }
                }
            }

            resolve(data);
        });
    })
};

/**
 * 爬蟲supply
 */
async function getTokenSupplys(contract_address) {
    return new Promise(function (resolve, reject) {
        request({
            url: `https://etherscan.io/token/generic-tokenholder-inventory?m=normal&contractAddress=${contract_address}&a=&pUrl=token`,
            headers: {
                'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/97.0.4692.71 Safari/537.36'
            },
            method: "GET"
        }, (error, res, body) => {
            // 如果有錯誤訊息，或沒有 body(內容)，就 return
            if (error || !body) {
                reject(err);
            }

            const data = [];
            const $ = cheerio.load(body); // 載入 body

            let supplys = $('div').eq(0).find('p').eq(0).text()
            let match1 = supplys.match(/(?<=A total of ).*(?= tokens found)/gs);

            if (match1) {
                supplys = match1[0]
                supplys = supplys.replace(',', '');
                supplys = parseInt(supplys);
            } else {
                supplys = 0
            }

            resolve(supplys);
        });
    })
};
/**
 * 爬蟲holders
 */
async function getTokenHolders(contract_address) {
    return new Promise(function (resolve, reject) {
        request({
            url: `https://etherscan.io/token/generic-tokenholders2?a=${contract_address}`,
            headers: {
                'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/97.0.4692.71 Safari/537.36'
            },
            method: "GET"
        }, (error, res, body) => {
            // 如果有錯誤訊息，或沒有 body(內容)，就 return
            if (error || !body) {
                reject(err);
            }

            const data = [];
            const $ = cheerio.load(body); // 載入 body


            let holders = $('#maintable').eq(0).find('#spinwheel').parent().text();
            let match1 = holders.match(/(?<=From a total of ).*(?= holder)/gs);
            let match2 = holders.match(/(?<=A total of ).*(?= token holder)/gs);

            if (match1) {
                holders = match1[0]
            } else {
                holders = match2[0]
            }
            holders = holders.replace(',', '');
            holders = parseInt(holders);

            resolve(holders);
        });
    })
};

/**
 * 爬蟲程序 core
 */
async function transactionsCrawler(url) {
    return new Promise(function (resolve, reject) {
        request({
            url: url,
            headers: {
                'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/97.0.4692.71 Safari/537.36'
            },
            method: "GET"
        }, (error, res, body) => {
            // 如果有錯誤訊息，或沒有 body(內容)，就 return
            if (error || !body) {
                reject(err);
            }

            const data = [];
            const $ = cheerio.load(body); // 載入 body
            const list = $("table tr");
            for (let i = 1; i < list.length; i++) {
                const td = list.eq(i).find('td');

                let from = td.eq(4).find('a').text();
                let tokenId = td.eq(7).find('a').text();

                if (!from.match('Null Address')) { //非鑄造
                    continue;
                } else if (parseInt(tokenId) > 50000) { //token數量過多，暫時排除
                    continue;
                } else {
                    let txn_hash = td.eq(1).find('a').text();
                    let timestamp = td.eq(2).find('span').text();
                    timestamp = Math.round(new Date(timestamp).getTime() / 1000);
                    timestamp = timestamp + 28800;

                    let token_id = parseInt(td.eq(7).find('a').text());
                    let token = td.eq(8).find('a');
                    let token_contract_address = token.attr('href').split('/')[2];
                    let long_name = token.find('span').find('font');
                    let nft_name = '';

                    // NFT名稱處理
                    if (long_name.length > 0) {
                        nft_name = token.find('span').attr('title');
                    } else {
                        nft_name = token.clone().children().remove().end().text();
                        nft_name = nft_name.split("(");
                        nft_name = nft_name[0];
                        nft_name = nft_name.trim();
                    }

                    if (nft_name != '') {
                        data.push({ nft_name, token_contract_address, txn_hash, token_id, timestamp });
                    }
                }
            }

            resolve(data);
        });
    })
};

function mysql_real_escape_string(str) {
    if (!str) return ''
    return str.replace(/[\0\x08\x09\x1a\n\r"'\\\%]/g, function (char) {
        switch (char) {
            case "\0":
                return "\\0";
            case "\x08":
                return "\\b";
            case "\x09":
                return "\\t";
            case "\x1a":
                return "\\z";
            case "\n":
                return "\\n";
            case "\r":
                return "\\r";
            case "\"":
            case "'":
            case "\\":
            case "%":
                return "\\" + char; // prepends a backslash to backslash, percent,
            // and double/single quotes
            default:
                return char;
        }
    });
}