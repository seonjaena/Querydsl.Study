package study.querydsl;

import com.querydsl.core.BooleanBuilder;
import com.querydsl.core.QueryResults;
import com.querydsl.core.Tuple;
import com.querydsl.core.types.ExpressionUtils;
import com.querydsl.core.types.Predicate;
import com.querydsl.core.types.Projections;
import com.querydsl.core.types.dsl.CaseBuilder;
import com.querydsl.core.types.dsl.Expressions;
import com.querydsl.jpa.JPAExpressions;
import com.querydsl.jpa.impl.JPAQueryFactory;
import org.assertj.core.api.Assertions;
import org.assertj.core.data.Percentage;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.annotation.Commit;
import org.springframework.transaction.annotation.Transactional;
import study.querydsl.dto.MemberDto;
import study.querydsl.dto.QMemberDto;
import study.querydsl.dto.UserDto;
import study.querydsl.entity.Member;
import study.querydsl.entity.QMember;
import study.querydsl.entity.Team;

import javax.persistence.EntityManager;
import javax.persistence.EntityManagerFactory;
import javax.persistence.PersistenceContext;
import javax.persistence.PersistenceUnit;

import java.util.List;

import static com.querydsl.jpa.JPAExpressions.*;
import static study.querydsl.entity.QMember.member;
import static study.querydsl.entity.QTeam.*;

@SpringBootTest
@Transactional
public class QuerydslBasicTest {

    @PersistenceContext
    private EntityManager em;

    /**
     * Multi-Thread 환경에서 문제 없이 동작하도록 설계되었다.
     * 주입 받는 EntityManager 또한 Multi-Thread 환경에서 문제 없이 동작하도록 설계되었다.
     */
    JPAQueryFactory query;

    @BeforeEach
    void before() {
        query = new JPAQueryFactory(em);

        Team teamA = new Team("teamA");
        Team teamB = new Team("teamB");
        em.persist(teamA);
        em.persist(teamB);

        Member member1 = new Member("member1", 10, teamA);
        Member member2 = new Member("member2", 20, teamA);
        Member member3 = new Member("member3", 30, teamB);
        Member member4 = new Member("member4", 40, teamB);
        em.persist(member1);
        em.persist(member2);
        em.persist(member3);
        em.persist(member4);
    }

    @Test
    void startJPQL() {
        // member1을 찾는다.
        Member findMember = em.createQuery("SELECT m FROM Member m where m.username = :username", Member.class)
                .setParameter("username", "member1")
                .getSingleResult();
        Assertions.assertThat(findMember.getUsername()).isEqualTo("member1");
    }

    @Test
    void startQuerydsl() {
        // member1을 찾는다.
        Member findMember = query
                .select(member)
                .from(member)
                .where(member.username.eq("member1"))    // 파라미터 바인딩 처리
                .fetchOne();

        assert findMember != null;
        Assertions.assertThat(findMember.getUsername()).isEqualTo("member1");
    }

    @Test
    void search() {
        Member findMember = query
                .selectFrom(member)
                .where(
                        member.username.eq("member1")
                                .and(member.age.between(10, 30))
                )
                .fetchOne();

        assert findMember != null;
        Assertions.assertThat(findMember.getUsername()).isEqualTo("member1");
    }

    @Test
    void searchAndParam() {
        Member findMember = query
                .selectFrom(member)
                .where(
                        member.username.eq("member1"),      // AND 조건
                        (member.age.eq(10))
                )
                .fetchOne();

        assert findMember != null;
        Assertions.assertThat(findMember.getUsername()).isEqualTo("member1");
    }

    @Test
    void resultFetchList() {
        List<Member> fetch = query
                .selectFrom(member)
                .fetch();

        assert fetch != null;
        Assertions.assertThat(fetch.size()).isSameAs(4);

        Member fetchOne = query
                .selectFrom(member)
                .offset(0)
                .limit(1)
                .orderBy(member.id.asc())
                .fetchOne();

        assert fetchOne != null;
        Assertions.assertThat(fetchOne.getUsername()).isEqualTo("member1");

        Member fetchFirst = query
                .selectFrom(member)
                .orderBy(member.id.asc())
                .fetchFirst();

        assert fetchFirst != null;
        Assertions.assertThat(fetchFirst.getUsername()).isEqualTo("member1");

        /**
         * // 쿼리를 두 번 실행함 (COUNT를 얻어오는 쿼리, 결과를 가져오는 쿼리) - deprecated 됨
         *         QueryResults<Member> fetchResults = query
         *                 .selectFrom(member)
         *                 .fetchResults();
         *         long total = fetchResults.getTotal();
         *         List<Member> results = fetchResults.getResults();
         */

        /**
         * // COUNT를 가져오는 쿼리 - deprecated 됨
         * long count = query
         *                 .selectFrom(member)
         *                 .fetchCount();
        */
    }

    /**
     * 회원 정렬 순서
     * 1. 회원 나이 내림차순
     * 2. 회원 이름 오름차순
     * 단 2에서 회원 이름이 없다면 마지막에 출력 (null last)
     */
    @Test
    void sort() {
        em.persist(new Member(null, 100));
        em.persist(new Member("member5", 100));
        em.persist(new Member("member6", 100));

        List<Member> result = query
                .selectFrom(member)
                .where(member.age.eq(100))
                .orderBy(
                        member.age.desc(),
                        member.username.asc().nullsLast()
                )
                .fetch();

        Member member5 = result.get(0);
        Member member6 = result.get(1);
        Member memberNull = result.get(2);

        Assertions.assertThat(member5.getUsername()).isEqualTo("member5");
        Assertions.assertThat(member6.getUsername()).isEqualTo("member6");
        Assertions.assertThat(memberNull.getUsername()).isNull();
    }

    @Test
    void paging1() {
        List<Member> result = query
                .selectFrom(member)
                .orderBy(member.username.desc())
                .offset(1)
                .limit(2)
                .fetch();

        Assertions.assertThat(result.size()).isSameAs(2);
    }

    @Test
    void paging2() {
        QueryResults<Member> queryResults = query
                .selectFrom(member)
                .orderBy(member.username.desc())
                .offset(1)
                .limit(2)
                .fetchResults();

        Assertions.assertThat(queryResults.getTotal()).isSameAs(4L);
        Assertions.assertThat(queryResults.getLimit()).isSameAs(2L);
        Assertions.assertThat(queryResults.getOffset()).isSameAs(1L);
        Assertions.assertThat(queryResults.getResults().size()).isEqualTo(2);
    }

    @Test
    void aggregation() {
        List<Tuple> result = query
                .select(
                        member.count(),
                        member.age.sum(),
                        member.age.avg(),
                        member.age.max(),
                        member.age.min()
                )
                .from(member)
                .fetch();

        Assertions.assertThat(result.size()).isSameAs(1);
        Tuple tuple = result.get(0);

        Assertions.assertThat(tuple.get(member.count())).isSameAs(4L);
        Assertions.assertThat(tuple.get(member.age.sum())).isSameAs(100);
        Assertions.assertThat(tuple.get(member.age.avg())).isCloseTo(25, Percentage.withPercentage(1));
        Assertions.assertThat(tuple.get(member.age.max())).isSameAs(40);
        Assertions.assertThat(tuple.get(member.age.min())).isSameAs(10);
    }

    /**
     * 팀의 이름과 각 팀의 평균 연령을 구한다.
     */
    @Test
    void group() {
        List<Tuple> result = query
                .select(team.name, member.age.avg())
                .from(member)
                .join(member.team, team)
                .groupBy(team.name)
                .having(member.age.avg().gt(2))
                .fetch();

        Tuple teamA = result.get(0);
        Tuple teamB = result.get(1);

        Assertions.assertThat(teamA.get(team.name)).isEqualTo("teamA");
        Assertions.assertThat(teamA.get(member.age.avg())).isCloseTo(15, Percentage.withPercentage(1));

        Assertions.assertThat(teamB.get(team.name)).isEqualTo("teamB");
        Assertions.assertThat(teamB.get(member.age.avg())).isCloseTo(35, Percentage.withPercentage(1));
    }

    /**
     * 팀 A에 소속된 모든 회원
     */
    @Test
    void join() {
        // given
        List<Member> result = query
                .selectFrom(member)
                .innerJoin(member.team, team)
                .where(team.name.eq("teamA"))
                .fetch();

        Assertions.assertThat(result)
                .extracting("username")
                .containsExactly("member1", "member2");
    }

    /**
     * 세타 조인
     * 회원의 이름이 팀 이름과 같은 회원 조인
     */
    @Test
    void theta_join() {
        em.persist(new Member("teamA"));
        em.persist(new Member("teamB"));
        em.persist(new Member("teamC"));

        List<Member> result = query
                .select(member)
                .from(member, team)
                .where(member.username.eq(team.name))
                .fetch();

        Assertions.assertThat(result)
                .extracting("username")
                .containsExactly("teamA", "teamB");
    }

    /**
     * 회원과 팀을 조인하면서, 팀 이름이 teamA인 팀만 조인, 회원은 모두 조회
     * JPQL: SELECT m FROM Member m LEFT JOIN m.team t ON t ON t.name = 'teamA'
     */
    @Test
    void join_on_filtering() {
        List<Tuple> result = query
                .select(member, team)
                .from(member)
                .leftJoin(member.team, team)
                .on(team.name.eq("teamA"))
                .fetch();

        Assertions.assertThat(result.size()).isSameAs(4);
        Assertions.assertThat(result.get(2).get(team)).isNull();
        Assertions.assertThat(result.get(3).get(team)).isNull();
    }

    /**
     * 연관관계가 없는 엔티티 외부 조인
     * 회원의 이름이 팀 이름과 같은 대상 외부 조인
     */
    @Test
    void join_on_no_relation() {
        em.persist(new Member("teamA"));
        em.persist(new Member("teamB"));
        em.persist(new Member("teamC"));

        List<Tuple> result = query
                .select(member, team)
                .from(member)
                .leftJoin(team)
                .on(member.username.eq(team.name))
                .fetch();

        result.forEach(System.out::println);
    }

    @PersistenceUnit
    EntityManagerFactory emf;

    @Test
    void fetchJoinNo() {
        em.flush();
        em.clear();

        Member findMember = query
                .selectFrom(member)
                .where(member.username.eq("member1"))
                .fetchOne();

        boolean loaded = emf.getPersistenceUnitUtil().isLoaded(findMember.getTeam());
        Assertions.assertThat(loaded).as("페치 조인 미적용").isFalse();
    }

    @Test
    void fetchJoinUse() {
        em.flush();
        em.clear();

        Member findMember = query
                .selectFrom(member)
                .join(member.team, team)
                .fetchJoin()
                .where(member.username.eq("member1"))
                .fetchOne();

        boolean loaded = emf.getPersistenceUnitUtil().isLoaded(findMember.getTeam());
        Assertions.assertThat(loaded).as("페치 조인 적용").isTrue();
    }

    /**
     * 나이가 가장 많은 회원 조회
     */
    @Test
    void subQuery() {
        // Sub Query의 Alias가 달라야하기 때문에 따로 QMember 생성
        QMember memberSub = new QMember("memberSub");

        List<Member> result = query
                .selectFrom(member)
                .where(member.age.eq(
                        select(memberSub.age.max())
                                .from(memberSub)
                ))
                .fetch();

        Assertions.assertThat(result.size()).isSameAs(1);
        Assertions.assertThat(result.get(0).getAge()).isSameAs(40);
    }

    /**
     * 나이가 평균 이상인 회원 조회
     */
    @Test
    void subQueryGoe() {
        // Sub Query의 Alias가 달라야하기 때문에 따로 QMember 생성
        QMember memberSub = new QMember("memberSub");

        List<Member> result = query
                .selectFrom(member)
                .where(member.age.goe(
                        select(memberSub.age.avg())
                                .from(memberSub)
                ))
                .fetch();

        Assertions.assertThat(result.size()).isSameAs(2);
        Assertions.assertThat(result).extracting("age")
                .containsExactly(30, 40);
    }

    /**
     * 나이가 10살 보다 많은 회원 조회
     */
    @Test
    void subQueryIn() {
        // Sub Query의 Alias가 달라야하기 때문에 따로 QMember 생성
        QMember memberSub = new QMember("memberSub");

        List<Member> result = query
                .selectFrom(member)
                .where(member.age.in(
                        select(memberSub.age)
                                .from(memberSub)
                                .where(memberSub.age.gt(10))
                ))
                .fetch();

        Assertions.assertThat(result.size()).isSameAs(3);
        Assertions.assertThat(result).extracting("age")
                .containsExactly(20, 30, 40);
    }

    @Test
    void selectSubQuery() {

        QMember memberSub = new QMember("memberSub");

        List<Tuple> result = query
                .select(
                        member.username,
                        select(memberSub.age.avg())
                                .from(memberSub)
                )
                .from(member)
                .fetch();

        result.forEach(System.out::println);
    }

    @Test
    void basicCase() {
        List<String> result = query
                .select(
                        member.age
                                .when(10).then("열살")
                                .when(20).then("스무살")
                                .otherwise("기타")
                )
                .from(member)
                .orderBy(member.age.asc())
                .fetch();

        Assertions.assertThat(result.size()).isSameAs(4);
        Assertions.assertThat(result.get(0)).isEqualTo("열살");
        Assertions.assertThat(result.get(1)).isEqualTo("스무살");
        Assertions.assertThat(result.get(2)).isEqualTo("기타");
        Assertions.assertThat(result.get(3)).isEqualTo("기타");
    }

    @Test
    void complexCase() {
        List<String> result = query
                .select(
                        new CaseBuilder()
                                .when(member.age.between(0, 20)).then("0~20살")
                                .when(member.age.between(21, 30)).then("21~30살")
                                .otherwise("기타")
                )
                .from(member)
                .orderBy(member.age.asc())
                .fetch();

        Assertions.assertThat(result.size()).isSameAs(4);
        Assertions.assertThat(result.get(0)).isEqualTo("0~20살");
        Assertions.assertThat(result.get(1)).isEqualTo("0~20살");
        Assertions.assertThat(result.get(2)).isEqualTo("21~30살");
        Assertions.assertThat(result.get(3)).isEqualTo("기타");
    }
    
    @Test
    void constant() {
        List<Tuple> result = query
                .select(member.username, Expressions.constant("A"))
                .from(member)
                .fetch();
        
        Assertions.assertThat(result.size()).isSameAs(4);
        Assertions.assertThat(result.get(0).get(member.username)).isEqualTo("member1");
        Assertions.assertThat(result.get(1).get(member.username)).isEqualTo("member2");
        Assertions.assertThat(result.get(2).get(member.username)).isEqualTo("member3");
        Assertions.assertThat(result.get(3).get(member.username)).isEqualTo("member4");

        Assertions.assertThat(result.get(0).get(Expressions.constant("A"))).isEqualTo("A");
    }

    @Test
    void concat() {
        // {username}_{age}
        List<String> result = query
                .select(member.username.concat("_").concat(member.age.stringValue()))
                .from(member)
                .where(member.username.eq("member1"))
                .fetch();

        Assertions.assertThat(result.size()).isSameAs(1);
        Assertions.assertThat(result.get(0)).isEqualTo("member1_10");
    }

    @Test
    void test() {
        query
                .select(member.age.stringValue())
                .from(member)
                .fetch();
    }

    @Test
    void simpleProjection() {
        List<Member> result = query
                .select(member)
                .from(member)
                .fetch();

        Assertions.assertThat(result.size()).isSameAs(4);
    }

    @Test
    void tupleProjection() {
        List<Tuple> result = query
                .select(member.username, member.age)
                .from(member)
                .orderBy(member.age.asc())
                .fetch();

        Assertions.assertThat(result.size()).isSameAs(4);
        Assertions.assertThat(result.get(0).get(member.username)).isEqualTo("member1");
        Assertions.assertThat(result.get(1).get(member.username)).isEqualTo("member2");
        Assertions.assertThat(result.get(2).get(member.username)).isEqualTo("member3");
        Assertions.assertThat(result.get(3).get(member.username)).isEqualTo("member4");
    }

    @Test
    void findDtoSPQL() {
        List<MemberDto> result = em.createQuery("SELECT new study.querydsl.dto.MemberDto(m.username, m.age) FROM Member m ", MemberDto.class)
                .getResultList();

        Assertions.assertThat(result.size()).isSameAs(4);
    }

    @Test
    void findDtoBySetter() {
        List<MemberDto> result = query
                .select(Projections.bean(MemberDto.class,
                        member.username,
                        member.age))
                .from(member)
                .fetch();

        result.forEach(System.out::println);

        Assertions.assertThat(result.size()).isSameAs(4);
    }

    @Test
    void findDtoByFideld() {
        List<MemberDto> result = query
                .select(Projections.fields(MemberDto.class,
                        member.username,
                        member.age))
                .from(member)
                .fetch();

        result.forEach(System.out::println);

        Assertions.assertThat(result.size()).isSameAs(4);
    }

    @Test
    void findDtoByConstructor() {
        List<MemberDto> result = query
                .select(Projections.constructor(MemberDto.class,
                        member.username,
                        member.age))
                .from(member)
                .fetch();

        result.forEach(System.out::println);

        Assertions.assertThat(result.size()).isSameAs(4);
    }

    @Test
    void findUserDtoBySetter() {

        QMember memberSub = new QMember("memberSub");

        List<UserDto> result = query
                .select(Projections.bean(UserDto.class,
                        member.username.as("name"),
                        ExpressionUtils.as(
                                select(memberSub.age.max())
                                        .from(memberSub), "userage"
                        )
                ))
                .from(member)
                .fetch();

        result.forEach(System.out::println);
        Assertions.assertThat(result.size()).isSameAs(4);
    }

    @Test
    void findUserDtoByFideld() {

        QMember memberSub = new QMember("memberSub");

        List<UserDto> result = query
                .select(Projections.fields(UserDto.class,
                        member.username.as("name"),
                        ExpressionUtils.as(
                                select(memberSub.age.max())
                                        .from(memberSub), "userage"
                        )
                ))
                .from(member)
                .fetch();

        result.forEach(System.out::println);
        Assertions.assertThat(result.size()).isSameAs(4);
    }

    @Test
    void findUserDtoByConstructor() {
        QMember memberSub = new QMember("memberSub");

        List<UserDto> result = query
                .select(Projections.constructor(
                        UserDto.class,
                        member.username,
                        select(memberSub.age.max())
                                .from(memberSub)
                ))
                .from(member)
                .fetch();

        result.forEach(System.out::println);
        Assertions.assertThat(result.size()).isSameAs(4);
    }

    @Test
    void findDtoByQueryProjection() {
        List<MemberDto> result = query
                .select(new QMemberDto(member.username, member.age))
                .from(member)
                .fetch();

        result.forEach(System.out::println);
        Assertions.assertThat(result.size()).isSameAs(4);
    }

    @Test
    void dynamicQuery_BooleanBuilder() {
        String usernameParam = "member1";
        Integer ageParam = null;

        List<Member> result = searchMember1(usernameParam, ageParam);
        Assertions.assertThat(result.size()).isEqualTo(1);
    }

    private List<Member> searchMember1(String usernameCond, Integer ageCond) {

        BooleanBuilder builder = new BooleanBuilder();
        if(usernameCond != null) {
            builder.and(member.username.eq(usernameCond));
        }

        if(ageCond != null) {
            builder.and(member.age.eq(ageCond));
        }

        return query
                .selectFrom(member)
                .where(builder)
                .fetch();
    }

    @Test
    void dynamicQuery_WhereParam() {
        String usernameParam = "member1";
        Integer ageParam = 10;

        List<Member> result = searchMember2(usernameParam, ageParam);
        Assertions.assertThat(result.size()).isEqualTo(1);
    }

    private List<Member> searchMember2(String usernameCond, Integer ageCond) {
        return query
                .selectFrom(member)
                .where(usernameEq(usernameCond), ageEq(ageCond))
                .fetch();
    }

    private Predicate usernameEq(String usernameCond) {
        return usernameCond != null ? member.username.eq(usernameCond) : null;
    }

    private Predicate ageEq(Integer ageCond) {
        return ageCond != null ? member.age.eq(ageCond) : null;
    }

    @Test
    void bulkUpdate() {
        /**
         * 벌크 연산은 영속성 컨텍스트에 값을 업데이트 하지 않는다.
         * 기존 member1, member2의 username은 '비회원'이 되기를 기대하지만, DB에만 값이 업데이트되고 영속성 컨텍스트에는 반영하지 않는다.
         * 따라서 밑에서 SELECT를 하더라도 영속성 컨텍스트의 값을 가져오기 때문에 member1, member2가 나온다.
         */
        long count = query
                .update(member)
                .set(member.username, "비회원")
                .where(member.age.lt(28))
                .execute();

        Assertions.assertThat(count).isSameAs(2L);
        /**
         * 영속성 컨텍스트와 DB의 데이터가 맞지 않기 때문에 영속성 컨텍스트를 한 번 비워준다.
         * 이 로직이 없다면, 밑의 검증 부분에서 실패한다.
         */
        em.flush();
        em.clear();

        List<Member> result = query
                .selectFrom(member)
                .orderBy(member.age.asc())
                .fetch();

        Assertions.assertThat(result.get(0).getUsername()).isEqualTo("비회원");
        Assertions.assertThat(result.get(1).getUsername()).isEqualTo("비회원");
        Assertions.assertThat(result.get(2).getUsername()).isEqualTo("member3");
        Assertions.assertThat(result.get(3).getUsername()).isEqualTo("member4");
    }

    @Test
    void bulkAdd() {
        long count = query
                .update(member)
                .set(member.age, member.age.add(1))
                .execute();

        Assertions.assertThat(count).isSameAs(4L);
    }

    @Test
    void bulkMultiply() {
        long count = query
                .update(member)
                .set(member.age, member.age.multiply(2))
                .execute();

        Assertions.assertThat(count).isSameAs(4L);
    }

    @Test
    void bulkDelete() {
        long count = query
                .delete(member)
                .where(member.age.gt(18))
                .execute();

        Assertions.assertThat(count).isSameAs(3L);
    }

    @Test
    void sqlFunction1() {
        List<String> result = query
                .select(
                        Expressions.stringTemplate("function('replace', {0}, {1}, {2})", member.username, "member", "M")
                )
                .from(member)
                .fetch();

        result.forEach(System.out::println);
    }

    @Test
    void sqlFunction2() {
        List<String> result = query
                .select(member.username)
                .from(member)
                .where(
                        member.username.eq(Expressions.stringTemplate("function('lower', {0})", member.username))
                )
                .fetch();

        result.forEach(System.out::println);
    }

    /**
     * teamB의 사용자 중 가장 나이가 많은 사람과 나이가 적은 사람 조회
     */
    @Test
    void subQueryTest1() {

        Team teamA = query
                .selectFrom(team)
                .where(team.name.eq("teamA"))
                .fetchOne();

        Team teamB = query
                .selectFrom(team)
                .where(team.name.eq("teamB"))
                .fetchOne();

        Team teamC = new Team("teamC");


        Member member5 = new Member("member5", 13, teamC);
        Member member6 = new Member("member6", 21, teamC);
        Member member7 = new Member("member7", 15, teamC);

        Member member8 = new Member("member8", 13, teamA);
        Member member9 = new Member("member9", 21, teamA);
        Member member10 = new Member("member10", 15, teamA);

        Member member11 = new Member("member11", 8, teamB);
        Member member12 = new Member("member12", 48, teamB);
        Member member13 = new Member("member13", 15, teamB);

        QMember oldMember = new QMember("OldMember");
        QMember youngMember = new QMember("YoungMember");

        em.persist(teamC);
        em.persist(member5);
        em.persist(member6);
        em.persist(member7);
        em.persist(member8);
        em.persist(member9);
        em.persist(member10);
        em.persist(member11);
        em.persist(member12);
        em.persist(member13);

        List<Member> result = query
                .select(member)
                .from(member)
                .join(member.team, team)
                .on(team.name.eq(teamB.getName()))
                .where(
                        member.age.eq(
                                select(youngMember.age.min())
                                        .from(youngMember)
                        ).or(
                                member.age.eq(
                                        select(oldMember.age.max())
                                                .from(oldMember)
                                )
                        )
                )
                .fetch();

        result.forEach(r -> System.out.println("Member = " + r));

    }

}
