package org.thoughts.on.java.date;

import java.util.Arrays;
import java.util.List;

import javax.persistence.EntityManager;
import javax.persistence.EntityManagerFactory;
import javax.persistence.Persistence;

import org.apache.log4j.Logger;
import org.hibernate.MultiIdentifierLoadAccess;
import org.hibernate.Session;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.thoughts.on.java.model.PersonEntity;

public class TestMultiLoad {

	Logger log = Logger.getLogger(this.getClass().getName());

	private EntityManagerFactory emf;

	@Before
	public void init() {
		emf = Persistence.createEntityManagerFactory("my-persistence-unit");
	}

	@After
	public void close() {
		emf.close();
	}

	@Test
	public void testJpaFind() {
		log.info("... testJpaFind ...");

		EntityManager em = emf.createEntityManager();
		em.getTransaction().begin();
		
		for (Long id : new Long[]{1L, 2L, 3L}) {
			PersonEntity person = em.find(PersonEntity.class, id);
			log.info("Person: "+person);
		}
		
		em.getTransaction().commit();
		em.close();
	}
	
	@Test
	public void testJpaInStatement() {
		log.info("... testJpaInStatement ...");

		EntityManager em = emf.createEntityManager();
		em.getTransaction().begin();
		
		List<Long> ids = Arrays.asList(new Long[]{1L, 2L, 3L});
		List<PersonEntity> persons = em.createQuery("SELECT p FROM Person p WHERE p.id IN :ids").setParameter("ids", ids).getResultList();
		
		for (PersonEntity person : persons) {
			log.info("Person: "+person);
		}
		
		em.getTransaction().commit();
		em.close();
	}
	
	@Test
	public void testMultiLoad() {
		log.info("... testMultiLoad ...");

		EntityManager em = emf.createEntityManager();
		em.getTransaction().begin();
		
		Session session = em.unwrap(Session.class);
		
		MultiIdentifierLoadAccess<PersonEntity> multiLoadAccess = session.byMultipleIds(PersonEntity.class);
		List<PersonEntity> persons = multiLoadAccess.multiLoad(1L, 2L, 3L);
		
		for (PersonEntity person : persons) {
			log.info("Person: "+person);
		}
		
		em.getTransaction().commit();
		em.close();
	}
	
	@Test
	public void testMultiLoadNoSessionCheck() {
		log.info("... testMultiLoadNoSessionCheck ...");

		EntityManager em = emf.createEntityManager();
		em.getTransaction().begin();
		
		PersonEntity p = em.find(PersonEntity.class, 1L);
		p.setFirstName("changed");
		
		Session session = em.unwrap(Session.class);
		
		List<PersonEntity> persons = session.byMultipleIds(PersonEntity.class).multiLoad(1L, 2L, 3L);
		
		for (PersonEntity person : persons) {
			log.info("Person: "+person);
		}
		
		em.getTransaction().commit();
		em.close();
	}
	
	@Test
	public void testMultiLoadSessionCheck() {
		log.info("... testMultiLoadSessionCheck ...");

		EntityManager em = emf.createEntityManager();
		em.getTransaction().begin();
		
		PersonEntity p = em.find(PersonEntity.class, 1L);
		log.info("Fetched PersonEntity with id 1");
		
		Session session = em.unwrap(Session.class);
		
		List<PersonEntity> persons = session.byMultipleIds(PersonEntity.class).enableSessionCheck(true).multiLoad(1L, 2L, 3L);
		
		for (PersonEntity person : persons) {
			log.info("Person: "+person);
		}
		
		em.getTransaction().commit();
		em.close();
	}
	
	@Test
	public void testMultiLoadBatchSize() {
		log.info("... testMultiLoadBatchSize ...");

		EntityManager em = emf.createEntityManager();
		em.getTransaction().begin();
		
		Session session = em.unwrap(Session.class);
		
		List<PersonEntity> persons = session.byMultipleIds(PersonEntity.class).withBatchSize(2).multiLoad(1L, 2L, 3L);
		
		for (PersonEntity person : persons) {
			log.info("Person: "+person);
		}
		
		em.getTransaction().commit();
		em.close();
	}
}
