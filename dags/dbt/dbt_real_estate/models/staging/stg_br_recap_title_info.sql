with source_br_recap_title_info as (
    select * from {{ source('src_trino', 'source_br_recap_title_info') }}
),
nullif_br_recap_title_info as (
    SELECT 	
        a.rnum -- 순번
        , trim(a.platPlc) platPlc -- 대지위치
        , a.sigunguCd -- 시군구코드
        , a.bjdongCd -- 법정동코드
        , a.platGbCd -- 대지구분코드
        , a.bun -- 번
        , a.ji -- 지
        , a.mgmBldrgstPk -- 관리건축물대장PK
        , a.regstrGbCd -- 대장구분코드
        , a.regstrGbCdNm -- 대장구분코드명
        , a.regstrKindCd -- 대장종류코드
        , a.regstrKindCdNm -- 대장종류코드명
        , a.newOldRegstrGbCd -- 신구대장구분코드
        , a.newOldRegstrGbCdNm -- 신구대장구분코드명
        , nullif(trim(a.newPlatPlc),'') newPlatPlc -- 도로명대지위치
        , nullif(trim(a.bldNm),'') bldNm -- 건물명
        , nullif(trim(a.splotNm),'') splotNm -- 특수지명
        , nullif(trim(a.block),'') block -- 블록
        , nullif(trim(a.lot),'') lot -- 로트
        , a.bylotCnt -- 외필지수
        , nullif(trim(a.naRoadCd),'') naRoadCd -- 새주소도로코드
        , nullif(trim(a.naBjdongCd),'') naBjdongCd -- 새주소법정동코드
        , nullif(trim(a.naUgrndCd),'') naUgrndCd -- 새주소지상지하코드
        , a.naMainBun -- 새주소본번
        , a.naSubBun -- 새주소부번
        , a.platArea -- 대지면적(㎡)
        , a.archArea -- 건축면적(㎡)
        , a.bcRat -- 건폐율(%)
        , a.totArea -- 연면적(㎡)
        , a.vlRatEstmTotArea -- 용적률산정연면적(㎡)
        , a.vlRat -- 용적률(%)
        , nullif(trim(a.mainPurpsCd),'') mainPurpsCd -- 주용도코드
        , nullif(trim(a.mainPurpsCdNm),'') mainPurpsCdNm -- 주용도코드명
        , nullif(trim(a.etcPurps),'') etcPurps -- 기타용도
        , a.hhldCnt -- 세대수(세대)
        , a.fmlyCnt -- 가구수(가구)
        , a.mainBldCnt -- 주건축물수
        , a.atchBldCnt -- 부속건축물수
        , a.atchBldArea -- 부속건축물면적(㎡)
        , a.totPkngCnt -- 총주차수
        , a.indrMechUtcnt -- 옥내기계식대수(대)
        , a.indrMechArea -- 옥내기계식면적(㎡)
        , a.oudrMechUtcnt -- 외부기계식대수(대)
        , a.oudrMechArea -- 외부기계식면적(㎡)
        , a.indrAutoUtcnt -- 옥내자주식대수(대)
        , a.indrAutoArea -- 옥내자주식면적(㎡)
        , a.oudrAutoUtcnt -- 외부자주식대수(대)
        , a.oudrAutoArea -- 외부자주식면적(㎡)
        , nullif(trim(a.pmsDay),'') pmsDay -- 허가일
        , nullif(trim(a.stcnsDay),'') stcnsDay -- 착공일
        , nullif(trim(a.useAprDay),'') useAprDay -- 사용승인일
        , nullif(trim(a.pmsnoYear),'') pmsnoYear -- 허가번호년
        , nullif(trim(a.pmsnoKikCd),'') pmsnoKikCd -- 허가번호기관코드
        , nullif(trim(a.pmsnoKikCdNm),'') pmsnoKikCdNm -- 허가번호기관코드명
        , nullif(trim(a.pmsnoGbCd),'') pmsnoGbCd -- 허가번호구분코드
        , nullif(trim(a.pmsnoGbCdNm),'') pmsnoGbCdNm -- 허가번호구분코드명
        , a.hoCnt -- 호수(호)
        , nullif(trim(a.engrGrade),'') engrGrade -- 에너지효율등급
        , a.engrRat -- 에너지절감율
        , a.engrEpi -- EPI점수
        , nullif(trim(a.gnBldGrade),'') gnBldGrade -- 친환경건축물등급
        , a.gnBldCert -- 친환경건축물인증점수
        , nullif(trim(a.itgBldGrade),'') itgBldGrade -- 지능형건축물등급
        , a.itgBldCert -- 지능형건축물인증점수
        , nullif(trim(a.crtnDay),'') crtnDay -- 생성일자
        , a.br_recap_title_info_id -- 유니크 해시 키
    from source_br_recap_title_info a
),
typecast_br_recap_title_info as (
    SELECT 
        cast(b.rnum as int) rnum -- 순번
        , b.platPlc -- 대지위치
        , b.sigunguCd -- 시군구코드
        , b.bjdongCd -- 법정동코드
        , b.platGbCd -- 대지구분코드
        , b.bun -- 번
        , b.ji -- 지
        , b.mgmBldrgstPk -- 관리건축물대장PK
        , b.regstrGbCd -- 대장구분코드
        , b.regstrGbCdNm -- 대장구분코드명
        , b.regstrKindCd -- 대장종류코드
        , b.regstrKindCdNm -- 대장종류코드명
        , b.newOldRegstrGbCd -- 신구대장구분코드
        , b.newOldRegstrGbCdNm -- 신구대장구분코드명
        , b.newPlatPlc -- 도로명대지위치
        , b.bldNm -- 건물명
        , b.splotNm -- 특수지명
        , b.block -- 블록
        , b.lot -- 로트
        , b.bylotCnt -- 외필지수
        , b.naRoadCd -- 새주소도로코드
        , b.naBjdongCd -- 새주소법정동코드
        , b.naUgrndCd -- 새주소지상지하코드
        , b.naMainBun -- 새주소본번
        , b.naSubBun -- 새주소부번
        , cast(b.platArea as double) platArea -- 대지면적(㎡)
        , cast(b.archArea as double) archArea -- 건축면적(㎡)
        , cast(b.bcRat as double) bcRat -- 건폐율(%)
        , cast(b.totArea as double) totArea -- 연면적(㎡)
        , cast(b.vlRatEstmTotArea as double) vlRatEstmTotArea -- 용적률산정연면적(㎡)
        , cast(b.vlRat as double) vlRat -- 용적률(%)
        , b.mainPurpsCd -- 주용도코드
        , b.mainPurpsCdNm -- 주용도코드명
        , b.etcPurps -- 기타용도
        , cast(b.hhldCnt as int) hhldCnt -- 세대수(세대)
        , cast(b.fmlyCnt as int) fmlyCnt -- 가구수(가구)
        , cast(b.mainBldCnt as int) mainBldCnt -- 주건축물수
        , cast(b.atchBldCnt as int) atchBldCnt -- 부속건축물수
        , cast(b.atchBldArea as double) atchBldArea -- 부속건축물면적(㎡)
        , cast(b.totPkngCnt as int) totPkngCnt -- 총주차수
        , cast(b.indrMechUtcnt as int) indrMechUtcnt -- 옥내기계식대수(대)
        , cast(b.indrMechArea as double) indrMechArea -- 옥내기계식면적(㎡)
        , cast(b.oudrMechUtcnt as int) oudrMechUtcnt -- 외부기계식대수(대)
        , cast(b.oudrMechArea as double) oudrMechArea -- 외부기계식면적(㎡)
        , cast(b.indrAutoUtcnt as int) indrAutoUtcnt -- 옥내자주식대수(대)
        , cast(b.indrAutoArea as double) indrAutoArea -- 옥내자주식면적(㎡)
        , cast(b.oudrAutoUtcnt as int) oudrAutoUtcnt -- 외부자주식대수(대)
        , cast(b.oudrAutoArea as double) oudrAutoArea -- 외부자주식면적(㎡)
        , b.pmsDay -- 허가일
        , {{ parse_valid_date('pmsDay') }} pmsDay_parsed -- 허가일
        , b.stcnsDay -- 착공일
        , {{ parse_valid_date('stcnsDay') }} stcnsDay_parsed -- 착공일
        , b.useAprDay -- 사용승인일
        , {{ parse_valid_date('useAprDay') }} useAprDay_parsed -- 사용승인일
        , b.pmsnoYear -- 허가번호년
        , b.pmsnoKikCd -- 허가번호기관코드
        , b.pmsnoKikCdNm -- 허가번호기관코드명
        , b.pmsnoGbCd -- 허가번호구분코드
        , b.pmsnoGbCdNm -- 허가번호구분코드명
        , cast(b.hoCnt as int) hoCnt -- 호수(호)
        , b.engrGrade -- 에너지효율등급
        , cast(b.engrRat as double) engrRat -- 에너지절감율
        , cast(b.engrEpi as double) engrEpi -- EPI점수
        , b.gnBldGrade -- 친환경건축물등급
        , cast(b.gnBldCert as double) gnBldCert -- 친환경건축물인증점수
        , b.itgBldGrade -- 지능형건축물등급
        , cast(b.itgBldCert as double) itgBldCert -- 지능형건축물인증점수
        , b.crtnDay -- 생성일자
        , {{ parse_valid_date('crtnDay') }} crtnDay_parsed -- 생성일자
        , b.br_recap_title_info_id -- 유니크 해시 키
    from nullif_br_recap_title_info b
),
final as (
    SELECT 
        c.sigunguCd -- 시군구코드
        , c.bjdongCd -- 법정동코드
        , c.bun -- 번
        , c.ji -- 지
        , c.platPlc -- 대지위치
        , c.platGbCd -- 대지구분코드
        , c.bldNm -- 건물명
        , c.mgmBldrgstPk -- 관리건축물대장PK
        , c.regstrGbCd -- 대장구분코드
        , c.regstrGbCdNm -- 대장구분코드명
        , c.regstrKindCd -- 대장종류코드
        , c.regstrKindCdNm -- 대장종류코드명
        , c.newOldRegstrGbCd -- 신구대장구분코드
        , c.newOldRegstrGbCdNm -- 신구대장구분코드명
        , c.newPlatPlc -- 도로명대지위치
        , c.splotNm -- 특수지명
        , c.block -- 블록
        , c.lot -- 로트
        , c.bylotCnt -- 외필지수
        , c.naRoadCd -- 새주소도로코드
        , c.naBjdongCd -- 새주소법정동코드
        , c.naUgrndCd -- 새주소지상지하코드
        , c.naMainBun -- 새주소본번
        , c.naSubBun -- 새주소부번
        , c.platArea -- 대지면적(㎡)
        , c.archArea -- 건축면적(㎡)
        , c.bcRat -- 건폐율(%)
        , c.totArea -- 연면적(㎡)
        , c.vlRatEstmTotArea -- 용적률산정연면적(㎡)
        , c.vlRat -- 용적률(%)
        , c.mainPurpsCd -- 주용도코드
        , c.mainPurpsCdNm -- 주용도코드명
        , c.etcPurps -- 기타용도
        , c.hhldCnt -- 세대수(세대)
        , c.fmlyCnt -- 가구수(가구)
        , c.mainBldCnt -- 주건축물수
        , c.atchBldCnt -- 부속건축물수
        , c.atchBldArea -- 부속건축물면적(㎡)
        , c.totPkngCnt -- 총주차수
        , c.indrMechUtcnt -- 옥내기계식대수(대)
        , c.indrMechArea -- 옥내기계식면적(㎡)
        , c.oudrMechUtcnt -- 외부기계식대수(대)
        , c.oudrMechArea -- 외부기계식면적(㎡)
        , c.indrAutoUtcnt -- 옥내자주식대수(대)
        , c.indrAutoArea -- 옥내자주식면적(㎡)
        , c.oudrAutoUtcnt -- 외부자주식대수(대)
        , c.oudrAutoArea -- 외부자주식면적(㎡)
        , c.pmsDay -- 허가일
        , c.pmsDay_parsed -- 허가일
        , c.stcnsDay -- 착공일
        , c.stcnsDay_parsed -- 착공일
        , c.useAprDay -- 사용승인일
        , c.useAprDay_parsed -- 사용승인일
        , c.pmsnoYear -- 허가번호년
        , c.pmsnoKikCd -- 허가번호기관코드
        , c.pmsnoKikCdNm -- 허가번호기관코드명
        , c.pmsnoGbCd -- 허가번호구분코드
        , c.pmsnoGbCdNm -- 허가번호구분코드명
        , c.hoCnt -- 호수(호)
        , c.engrGrade -- 에너지효율등급
        , c.engrRat -- 에너지절감율
        , c.engrEpi -- EPI점수
        , c.gnBldGrade -- 친환경건축물등급
        , c.gnBldCert -- 친환경건축물인증점수
        , c.itgBldGrade -- 지능형건축물등급
        , c.itgBldCert -- 지능형건축물인증점수
        , c.crtnDay -- 생성일자
        , c.crtnDay_parsed -- 생성일자
        , c.br_recap_title_info_id -- 유니크 해시 키
    from typecast_br_recap_title_info c
)
select * from final