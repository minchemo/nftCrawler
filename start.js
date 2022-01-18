var request = require("request");
var cheerio = require("cheerio");
var mysql = require('mysql');
var _ = require('lodash');
var { mysql_config, crawler_list } = require('./config.json');
var htmlDecode = require('decode-html');

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
    if (err) throw err;

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

        let nft_item_id = await checkNftItemExist(nft_data);
        if (nft_item_id == 0) {
            nft_item_id = await getNftItemId(nft_data);
        }

        let insert = await insertTransaction(nft_item_id, grouped[key]);
        // if (insert) {
        //     console.log(JSON.stringify(nft_data) + ' 已保存!');
        // }
    }


    console.log('已完成本次寫入，準備進行下一次抓取...');
    await deleteOldTransaction(); //清除舊紀錄
    console.log('已清除24小時前 Transactions');

    setTimeout(() => {
        startFetchTransactions(); //開始下一次
    }, 5000);
}

/**
 * 檢查nft_item存在
 */
async function checkNftItemExist(item) {
    return new Promise(function (resolve, reject) {
        var sql = "INSERT IGNORE INTO nft_item (contract_address, name) VALUES ?";
        var values = [
            [item.contract_address, htmlDecode(item.nft_name)]
        ];
        connection.query(sql, [values], function (err, result) {
            if (err) throw reject(err);
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
            if (err) throw reject(err);
            resolve(result[0].id);
        });
    })
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