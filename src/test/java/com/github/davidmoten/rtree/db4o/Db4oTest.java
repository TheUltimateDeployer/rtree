package com.github.davidmoten.rtree.db4o;

import java.io.File;

import org.junit.Assert;
import org.junit.Test;

import com.db4o.Db4oEmbedded;
import com.db4o.ObjectSet;
import com.db4o.config.EmbeddedConfiguration;
import com.db4o.config.encoding.StringEncodings;
import com.db4o.ext.ExtObjectContainer;
import com.db4o.query.Query;
import com.db4o.ta.TransparentActivationSupport;
import com.github.davidmoten.rtree.RTree;
import com.github.davidmoten.rtree.geometry.Geometries;
import com.github.davidmoten.rtree.geometry.Point;

public class Db4oTest {

	@Test
	public void testContextIllegalMaxChildren() {
		File file = new File("test.db");
		ExtObjectContainer container = null;
		try {
			RTree<String, Point> tree = RTree.star().create();
			tree = tree.add("Sydney", Geometries.point(151.2094, -33.86));
			tree = tree.add("Brisbane", Geometries.point(153.0278, -27.4679));
			tree = tree.add("Bungendore", Geometries.point(149.4500, -35.2500));
			tree = tree.add("Canberra", Geometries.point(149.1244, -35.3075));

			EmbeddedConfiguration configuration = Db4oEmbedded.newConfiguration();
			configuration.common().add(new TransparentActivationSupport());
			//configuration.common().messageLevel(Integer.MAX_VALUE);
			configuration.common().exceptionsOnNotStorable(true);
			configuration.common().callbacks(false);
			configuration.common().stringEncoding(StringEncodings.utf8());
			configuration.file().recoveryMode(true);
			configuration.file().freespace().discardSmallerThan(Integer.MAX_VALUE);

			container = (ExtObjectContainer) Db4oEmbedded.openFile(configuration, file.getAbsolutePath());

			container.store(tree);
			tree.delete("Canberra", Geometries.point(149.1244, -35.3075));
			container.store(tree);

			Query query = container.query();
			query.constrain(RTree.class);
			ObjectSet<RTree<String, Point>> lstResult = query.execute();

			Assert.assertTrue("Search Result", lstResult.size() == 1);
			Assert.assertTrue("Children", lstResult.get(0).size() == 3);
		} catch (Exception e) {
			Assert.fail(e.toString());
		} finally {
			if (container != null && !container.isClosed()) {
				container.purge();
				while (!container.close())
					;
				container = null;
			}

			file.delete();
		}

	}
}
