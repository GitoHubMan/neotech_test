package com.neotech;

import com.neotech.util.PersistenceManager;
import org.junit.Test;

import javax.persistence.EntityManager;

public class MainTest {
    private EntityManager em = PersistenceManager.INSTANCE.getEntityManager();

}
