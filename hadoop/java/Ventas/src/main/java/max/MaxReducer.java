package max;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import writables.SucursalCantidadWritable;

public class MaxReducer extends Reducer<Text, SucursalCantidadWritable, Text, SucursalCantidadWritable> {

	public void reduce(Text key, Iterable<SucursalCantidadWritable> values, Context context)
			throws IOException, InterruptedException {

		Long max = Long.MIN_VALUE;
		ArrayList<String> sucursales = new ArrayList<>();
		for (SucursalCantidadWritable val : values) {
			if (val.getCantidad() > max) {
				sucursales.removeAll(sucursales);
				sucursales.add(val.getSucursal());
				max = val.getCantidad();
			} else if (val.getCantidad() == max) {
				sucursales.add(val.getSucursal());
			}
		}
		
		for (String suc : sucursales) {
			context.write(key, new SucursalCantidadWritable(suc, max));
		}
	}
}
