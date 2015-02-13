---
title: JSHS2 (Javascript HiveServer2)

language_tabs:
  - javascript

toc_footers:
  - <a href='http://github.com/tripit/slate'>Documentation Powered by Slate</a>

includes:
  - errors

search: true
---

# Introduction
Node.js 기반에서 Hive Server2에 접속하여 데이터를 쓰거나, 읽도록 할 수 있는 드라이버 프로그램이다.

## Connection
Hive Server2에 접속한다.

### connect
실제로 Hive Server에 접속하는 함수.

> 아래와 같이 사용할 수 있다.

```js
var conn = new Connection(option);

conn.connect(function () {
    aa
});
```
> 으하망ㅇㅇㄹ람ㅇㄹ