package soldProductsByOfficeMax;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import writables.ProductoCantidadWritable;
import writables.SucursalCantidadWritable;

public class MaxReducer extends Reducer<Text, ProductoCantidadWritable, Text, ProductoCantidadWritable> {

	public void reduce(Text key, Iterable<ProductoCantidadWritable> values, Context context)
			throws IOException, InterruptedException {

		Long max = Long.MIN_VALUE;
		ArrayList<String> productos = new ArrayList<>();
		for (ProductoCantidadWritable val : values) {
			if (val.getCantidad() > max) {
				productos.removeAll(productos);
				productos.add(val.getProducto());
				max = val.getCantidad();
			} else if (val.getCantidad() == max) {
				productos.add(val.getProducto());
			}
		}
		
		for (String prod : productos) {
			context.write(key, new ProductoCantidadWritable(prod, max));
		}
	}
}
