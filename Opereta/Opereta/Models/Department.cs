using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using System.Linq;
using System.Threading.Tasks;

namespace Opereta.Models
{
    public class Department
    {
        public Department()
        {
            this.Members = new HashSet<ApplicationUser>();
        }
        public int Id { get; set; }

        [Required]
        public string Name { get; set; }

        [Column(TypeName = "decimal(18,2)")]
        public Decimal Budget { get; set; }
        
        public virtual ApplicationUser HeadOfDepartment { get; set; }
        public virtual ICollection<ApplicationUser> Members { get; set; }
    }
}
